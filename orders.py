from shared.tools.logging import Logger
from shared.tools.global import ExtraGlobal
from shared.tools.thread import async
from shared.tools.error import python_full_stack

from eurosort.config import EuroSorterConfig
from eurosort.service import EuroSorterPolling, EuroSorterPermissivePolling
from eurosort.tracking.contents import EuroSorterContentTracking, Destination, Sides
from eurosort.tracking.lights import EuroSorterLightControl
from eurosort.tracking.wcs import EuroSorterAccessWCS
from eurosort.enums import MessageCode
from eurosort.helpers.tools import *

from eurosort.utility import now, seconds_since, coerce_to_set
from system.date import now as date_now
from datetime import datetime
import system
import re


get_dims = 'Dims'
get_max = 'Dims/Mode/Max'
get_min = 'Dims/Mode/Min'

# ---------------------------------------------------------------------------
# Regex + classification helpers
# ---------------------------------------------------------------------------

NOREAD_RE  = re.compile(r'^NoRead$')
NOSCAN_RE  = re.compile(r'^NoScanTX$')
INVALID_RE = re.compile(r'^NoCode$')

ERROR_ZONES = ['JACKPOT', 'NOREAD', 'UNRESOLVED']
NODEST = ['!!', '??']

# Tote: RCV###
TOTE_RE = re.compile(r'^RCV\d{3}$')
# DST: DST-0001..9999-1/2-1/2-A/B
DST_RE = re.compile(r'^DST-(?!0000)\d{4}-(1|2)-(1|2)-(A|B)$')
# SDR literal
SDR_RE = re.compile(r'^SDR$')
# IBN: 6 alphanumeric chars, BUT NOT a Tote (RCV###)
IBN_RE = re.compile(r'^(?!RCV\d{3}$)[A-Z0-9]{6}$')

# for Level 3 _route_noread
NOREAD_VALUES = set(['noread'])

error_matches = {
	NOREAD_RE:  "NOREAD",
	NOSCAN_RE:  "NOSCAN",
	INVALID_RE: "NOCODE",
}

code_matches = {
	DST_RE:  "DST",
	TOTE_RE: "TOTE",
	SDR_RE:  "SDR",
	IBN_RE:  "IBN",
}

lifespan = 60 * 60 * 24 * 7  # one week


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v, default=0.0):
	if v is None:
		return round(float(default), 2)
	try:
		return round(float(v), 2)
	except Exception:
		return round(float(default), 2)


def _volume(l, w, h):
	try:
		return round(float(l) * float(w) * float(h), 2)
	except Exception:
		return 0.0


# ===========================================================================
# LEVEL 3
# ===========================================================================

class Level_3_OrderRouting(
	EuroSorterContentTracking,
	EuroSorterPermissivePolling,
	EuroSorterPolling,
	EuroSorterAccessWCS,
	EuroSorterLightControl,
):
	CONTROL_PERMISSIVE_TAG_MAPPING = {
		'auto_active': 'Auto Unload Active',
		'group_by': 'Group_By',
		'sort_by': 'Sort_By',
		'max_fill': 'Max_Fill',
		'max_noread_recirc': 'No Read recirc attempts',
		'max_resort_recirc': 'recirc attempts',
		'extra_volume': 'Extra_Volume',
		'tote_main_volume': 'Tote_Volume_Overall',
		'tote_use_volume': 'Volume',
		'lane1_enabled': 'Chute_Control/Lane_1_Enabled',
		'lane2_enabled': 'Chute_Control/Lane_2_Enabled',
		'lane3_enabled': 'Chute_Control/Lane_3_Enabled',
		'lane4_enabled': 'Chute_Control/Lane_4_Enabled',
		'lane1_set': 'AutoAssign/Lane_1/Set',
		'lane2_set': 'AutoAssign/Lane_2/Set',
		'lane3_set': 'AutoAssign/Lane_3/Set',
		'lane4_set': 'AutoAssign/Lane_4/Set',
		'clear_all': 'Reset/Clear_all_Data',
		'clear_chute': 'Reset/Clear_Chute_Data',
		'chute_to_delete': 'Reset/Clear_Chute_Data',
		'squelch_WCS': 'Squelch all WCS updates',
	}

	def __init__(self, name, **init_cfg):
		super(Level_3_OrderRouting, self).__init__(name, **init_cfg)

		self.loggerInfo = system.util.getLogger("Level3_info")

		self.scan_counts = {
			'scanner': {'GoodRead': 0, 'NoRead': 0, 'TotalScans': 0, 'Rate': 0.0},
		}
		self.divert_counts = {
			'diverts': {
				'Confirmed': 0,
				'Confirmed_NoRead': 0,
				'Confirmed_Jackpot': 0,
				'Failed_Full': 0,
				'Failed_Wrong': 0,
				'Failed_Other': 0,
				'Total_Discharged': 0,
			}
		}

		self._last_check_processed_chutes = system.date.now()
		self._polling_methods.append(self._check_processed_chutes_periodic)
		self._polling_methods.append(self._assign_initial_error_chutes)
		self._polling_methods.append(self._get_chute_updates)

		self.last_sorted_lane = 1

		self._router_sequence = [
			self._route_order,
			self._route_noread,
			self._max_recirc,
		]

		for perm, tag in self.CONTROL_PERMISSIVE_TAG_MAPPING.items():
			self._subscribe_control_permissive(perm, tag)

		self._init_polling()

	# -------------------------------------------------------------------------
	# Shared destination helpers (new contents.py)
	# -------------------------------------------------------------------------

	def _dest_info(self, rec):
		if not isinstance(rec, dict):
			return {}
		info = rec.get('chute_info')
		return info if isinstance(info, dict) else {}

	def _dest_get(self, rec, key, default=None):
		if not isinstance(rec, dict):
			return default

		if key in rec:
			return rec.get(key, default)

		info = rec.get('chute_info')
		if isinstance(info, dict):
			return info.get(key, default)

		return default

	def _dest_update(self, destination, common_updates=None, chute_updates=None):
		common_updates = common_updates or {}
		chute_updates = chute_updates or {}

		current = self.destination_get(destination) or {}
		current_info = current.get('chute_info')
		if not isinstance(current_info, dict):
			current_info = {}

		merged = dict(common_updates)
		if chute_updates:
			merged['chute_info'] = dict(current_info, **chute_updates)

		if 'last_updated' not in merged:
			merged['last_updated'] = system.date.now()

		self.destination_update(destination, merged)

	# -------------------------------------------------------------------------
	# Small utilities
	# -------------------------------------------------------------------------

	def _safe_tag_write(self, paths, values):
		try:
			if isinstance(paths, (str, unicode)):
				paths = [paths]
			if not isinstance(values, (list, tuple)):
				values = [values]
			system.tag.writeBlocking(paths, values)
		except Exception as e:
			try:
				self.logger.error("Tag write failed for {}: {}".format(paths, e))
			except Exception:
				pass

	def _parse_list_field(self, field):
		if not field:
			return []
		if isinstance(field, (list, tuple)):
			return list(field)
		s = str(field).strip()
		if not s:
			return []
		return [x for x in s.split(',') if x != '']

	def _calculate_volume_metrics(self, curr, delta):
		new_vol = float(curr or 0.0) + float(delta or 0.0)
		tote_vol = float(self.get_permissive('tote_main_volume') or 1.0)
		percent = round((new_vol / tote_vol) * 100.0, 2) if tote_vol else 0.0
		return new_vol, percent

	def _calculate_volume_metrics_product(self, l, w, h):
		tote_vol = float(self.get_permissive('tote_main_volume') or 0.0)
		extra = float(self.get_permissive('extra_volume') or 0.0)

		orig = float(l or 0.0) * float(w or 0.0) * float(h or 0.0)
		if orig <= 0:
			return (tote_vol * (extra / 100.0))

		return (orig + (orig * extra / 100.0))

	def _resolve_issue(self, barcode_str):
		codes = str(barcode_str or '').split(',')
		return self.wcs_get_issue(codes)

	# -------------------------------------------------------------------------
	# Destination filtering
	# -------------------------------------------------------------------------

	def _match_cond(self, rec, cond):
		if not isinstance(cond, dict):
			return True

		for k, v in cond.items():
			if k == "$and":
				for sub in (v or []):
					if not self._match_cond(rec, sub):
						return False
				continue

			value = self._dest_get(rec, k, None)

			if isinstance(v, dict) and "$regex" in v:
				pat = v.get("$regex") or ""
				try:
					if re.match(pat, str(value or "")) is None:
						return False
				except Exception:
					return False
				continue

			if value != v:
				return False

		return True

	def _find_destinations(self, filt):
		out = []
		for _, rec in (self._destination_contents or {}).items():
			if not isinstance(rec, dict):
				continue
			if self._match_cond(rec, filt):
				out.append(rec)
		return out

	# -------------------------------------------------------------------------
	# Periodics / status
	# -------------------------------------------------------------------------

	def _get_chute_updates(self):
		try:
			chute_counts = self.get_chutes_updates()
			transit_counts = self.get_transit_updates()
			try:
				chute_counts[0].update(transit_counts[0])
			except Exception:
				pass

			self._safe_tag_write(
				'[EuroSort]EuroSort/Level3/Sorter_Control/Status/Counts',
				chute_counts
			)
		except Exception:
			return

	def _check_door_state(self):
		path_base = '[EuroSort]EuroSort/Level3/Destinations/'
		door_path = 'Destination/Chute_Door_Status'

		chutes = self._find_destinations({"$and": [{"in_service": True}]})
		for chute in chutes:
			destination = chute.get('_id') or chute.get('destination')
			if not destination:
				continue

			tag_path = '{0}{1}/{2}'.format(path_base, destination, door_path)
			exec_path = tag_path + '.Executed'

			try:
				door_status, exec_ = [
					qv.value for qv in system.tag.readBlocking([tag_path, exec_path])
				]
			except Exception:
				continue

			door_status = bool(door_status)
			exec_ = bool(exec_)

			if (not door_status) and (not exec_):
				self._safe_tag_write([exec_path], [True])
				self.log_event('Routing', reason='{}: door opened'.format(destination), ibn='', destination=destination, code=16)
				if bool(chute.get('occupied')):
					self.log_event(
						'Routing',
						reason='{}: items dropped on takeaway conveyor'.format(destination),
						ibn=self._dest_get(chute, 'ibns', ''),
						destination=destination,
						code=19
					)

			elif door_status and exec_:
				self._safe_tag_write([exec_path], [False])
				self.log_event('Routing', reason='{}: door closed'.format(destination), ibn='', destination=destination, code=17)
				if bool(self._dest_get(chute, 'waiting_for_processing', False)) and bool(chute.get('occupied')):
					self.log_event('Routing', reason='{}: waiting for processing'.format(destination), ibn='', destination=destination, code=18)
			else:
				self._safe_tag_write([exec_path], [False])

	def _check_processed_chutes_periodic(self):
		now_ts = system.date.now()

		if system.date.millisBetween(self._last_check_processed_chutes, now_ts) >= 60000:
			self._check_key_updates_for_chutes()

		if system.date.millisBetween(self._last_check_processed_chutes, now_ts) >= 120000:
			self._check_door_state()
			self._last_check_processed_chutes = now_ts

	def _check_key_updates_for_chutes(self):
		try:
			chutes = self.get_processing_status()
		except Exception:
			chutes = None

		if not chutes:
			return

		for chute in chutes:
			chute_id = chute.get('_id')
			if not chute_id:
				continue

			path = "[EuroSort]EuroSort/Level3/Destinations/{}/Destination/WCS_Processed".format(str(chute_id))
			self._safe_tag_write([path], [True])

			self.log_event('Routing', reason='{}: was processed to tote'.format(chute_id), destination=chute_id, code=20)
			self.log_event('Routing', reason='{}: was cleared and ready for new product'.format(chute_id), destination=chute_id, code=21)

	# -------------------------------------------------------------------------
	# Routing
	# -------------------------------------------------------------------------

	def _route_order(self, sorter_data):
		barcode = sorter_data.barcode
		issue = self._resolve_issue(barcode) or {}

		issue_id = str(issue.get('_id') or sorter_data.barcode or '')
		zone = str(issue.get('zone') or '')
		group_id = str(issue.get('group_id', '') or '')

		sorter_data.barcode = issue_id

		if issue_id.lower() in NOREAD_VALUES:
			self.scan_counts['scanner']['NoRead'] += 1
		else:
			self.scan_counts['scanner']['GoodRead'] += 1

		self.scan_counts['scanner']['TotalScans'] += 1
		total = float(self.scan_counts['scanner']['TotalScans'] or 0)
		good = float(self.scan_counts['scanner']['GoodRead'] or 0)
		self.scan_counts['scanner']['Rate'] = round((good / total) * 100.0, 2) if total else 0.0

		self._safe_tag_write('[EuroSort]EuroSort/Level3/Sorter_Control/Status/scan_counts', self.scan_counts)

		self.log_event('Routing', reason='Scanned {}'.format(issue_id), code=1, ibn=issue_id, destination='')
		self.log_event('Routing', reason='Looking for a chute matching Zone: {} and or Group_ID: {}'.format(zone, group_id), code=2, ibn='', destination='')

		destination = self._find_matching_chute(sorter_data, zone, group_id)

		if (not destination) and (zone not in ERROR_ZONES):
			self.log_event('Routing', reason='No chutes available for {}, getting next available.'.format(issue_id), code=5, ibn=issue_id, destination='')
			destination = self._get_next_available_chute(sorter_data, group_id, zone)
			if not destination:
				self.log_event('Routing', reason='No chutes available for {}'.format(issue_id), code=100, ibn=issue_id, destination='')

		return destination

	def _route_noread(self, sorter_data):
		if str(sorter_data.barcode or '').lower() not in NOREAD_VALUES:
			return None

		carrier_number = int(sorter_data.carrier_number)
		rec = self.carrier_get(carrier_number) or {}
		count = int(rec.get('recirculation_count', 0) or 0) + 1
		max_count = int(self.get_permissive('max_noread_recirc') or 0)

		self.carrier_update(carrier_number, {'recirculation_count': count})

		remaining = max_count - count
		try:
			self.logger.debug("NoRead recirc count is {}, will recirculate {} more times".format(count, remaining))
		except Exception:
			pass

		if max_count and count >= max_count:
			issue = (rec.get('issue_info') or {})
			ibn = issue.get('ibn') or issue.get('_id') or sorter_data.barcode
			self.log_event('Routing', reason='IBN: {} reached max recirc count routing to NoRead chute'.format(ibn), code=9, ibn=ibn, destination='')
			return self._find_matching_chute(sorter_data, 'NoRead', '-2')

		return None

	def _max_recirc(self, sorter_data):
		carrier_number = int(sorter_data.carrier_number)
		rec = self.carrier_get(carrier_number) or {}

		count = int(rec.get('recirculation_count', 0) or 0) + 1
		max_count = int(self.get_permissive('max_resort_recirc') or 0)

		self.carrier_update(carrier_number, {'recirculation_count': count})

		if max_count and count >= max_count:
			issue = (rec.get('issue_info') or {})
			ibn = issue.get('ibn') or issue.get('_id') or sorter_data.barcode
			self.log_event('Routing', reason='IBN: {} reached max recirc count forced to jackpot chute'.format(ibn), code=10, ibn=ibn, destination='')
			return self._find_matching_chute(sorter_data, 'Jackpot', '-3')

		return None

	def route_destination(self, sorter_data):
		try:
			for router in self._router_sequence:
				destination = router(sorter_data)
				if destination is not None:
					return destination
		except StopIteration as stop_looking:
			return stop_looking
		except Exception:
			return StopIteration

		return StopIteration

	# -------------------------------------------------------------------------
	# Error chute assignment
	# -------------------------------------------------------------------------

	def _assign_initial_error_chutes(self):
		lanes = ['lane1_enabled', 'lane2_enabled', 'lane3_enabled', 'lane4_enabled']

		for i, en in enumerate(lanes, start=1):
			if not bool(self.get_permissive(en)):
				continue

			lane_set = bool(self.get_permissive('lane{}_set'.format(i)))
			if lane_set:
				continue

			nr_tag = "{}/AutoAssign/Lane_{}/Max_Noread_Chutes".format(self.CONTROL_TAG_PATH, i)
			jp_tag = "{}/AutoAssign/Lane_{}/Max_Jackpot_Chutes".format(self.CONTROL_TAG_PATH, i)

			try:
				max_nr = int(system.tag.readBlocking([nr_tag])[0].value)
				max_jp = int(system.tag.readBlocking([jp_tag])[0].value)
			except Exception:
				max_nr, max_jp = 0, 0

			self._assign_error_chutes_for_lane(i, 'NoRead', max_nr)
			self._assign_error_chutes_for_lane(i, 'Jackpot', max_jp)

			self._safe_tag_write(['{}/AutoAssign/Lane_{}/Set'.format(self.CONTROL_TAG_PATH, i)], [True])

	def _assign_error_chutes_for_lane(self, lane, zone, count):
		filter_expr = {"$and": [
			{"lane": lane},
			{"zone": str(zone)},
			{"in_service": True},
		]}

		existing = self._find_destinations(filter_expr)
		needed = int(count or 0) - len(existing)

		self.log_event('Routing', reason='Lane {} changed from {} to {}'.format(lane, len(existing), count), code=27, ibn='', destination='')

		if needed <= 0:
			return

		filt = {"$and": [
			{"lane": lane},
			{"occupied": False},
			{"in_service": True},
			{"queued": False},
			{"faulted": False},
			{"wcs_processed": True},
		]}

		candidates = self._find_destinations(filt)
		if lane in (3, 4):
			candidates = candidates[::-1]

		for ch in candidates[:needed]:
			chute_id = ch.get('_id') or ch.get('destination')
			if not chute_id:
				continue

			group_id = "-2" if zone == "NoRead" else "-3"

			self.log_event(
				'Routing',
				reason='Assigned zone: {} and Group_Id: {} to chute:{}'.format(zone, group_id, chute_id),
				destination=chute_id,
				code=28,
				ibn=''
			)

			self._dest_update(
				chute_id,
				common_updates={
					'_id': chute_id,
					'occupied': True,
				},
				chute_updates={
					'zone': str(zone),
					'group_id': str(group_id),
					'wcs_processed': False,
				}
			)

	# -------------------------------------------------------------------------
	# Chute selection / assignment
	# -------------------------------------------------------------------------

	def _find_matching_chute(self, sorter_data, zone, group_id):
		if zone in ('NoRead', 'Jackpot'):
			group = 2
		else:
			group = int(self.get_permissive('group_by') or 0)

		group_query = {
			0: {'zone': str(zone)},
			1: {'group_id': str(group_id)},
			2: {'zone': str(zone), 'group_id': str(group_id)},
		}

		base_filt = {"$and": [
			group_query.get(group) or {},
			{"toteFull": False},
			{"occupied": True},
			{"in_service": True},
			{"queued": False},
			{"wcs_processed": False},
			{"waiting_for_processing": False},
			{"faulted": False},
		]}

		chutes = self._find_destinations(base_filt)
		if not chutes:
			return None

		for chute in chutes:
			destination = self._process_chute_result(chute, sorter_data, group_id, zone)
			if destination:
				return destination

		return None

	def _process_chute_result(self, chute_rec, sorter_data, group_id, zone):
		chute_id = chute_rec.get('_id') or chute_rec.get('destination')
		if not chute_id:
			return None

		carrier_number = int(sorter_data.carrier_number)

		cal_vol = self._calculate_volume_metrics_product(sorter_data.length, sorter_data.width, sorter_data.height)
		curr_volume = float(self._dest_get(chute_rec, 'volume', 0.0) or 0.0)
		new_volume, percent_full = self._calculate_volume_metrics(curr_volume, cal_vol)

		if new_volume >= float(self.get_permissive('tote_main_volume') or 0.0):
			return None

		issue_id = str(sorter_data.barcode or '')

		zones = self._parse_list_field(self._dest_get(chute_rec, 'zone', ''))
		ibns = self._parse_list_field(self._dest_get(chute_rec, 'ibns', ''))
		groups = self._parse_list_field(self._dest_get(chute_rec, 'group_id', ''))

		if zone and zone not in zones:
			zones.append(zone)
		if issue_id and issue_id not in ibns:
			ibns.append(issue_id)
		if group_id and str(group_id) not in groups:
			groups.append(str(group_id))

		tote_full = True if float(percent_full) >= float(self.get_permissive('max_fill') or 0.0) else False

		dest_updates_common = {
			'_id': chute_id,
			'occupied': True,
		}

		dest_updates_chute = {
			'volume': float(new_volume),
			'volume_percent_full': float(percent_full),
			'toteFull': bool(tote_full),
			'zone': ','.join(zones),
			'ibns': ','.join(ibns),
			'group_id': ','.join(groups),
			'wcs_processed': False,
			'waiting_for_processing': False,
		}

		issue_info = {
			'ibn': issue_id,
			'zone': zone,
			'group_id': str(group_id),
			'length': sorter_data.length,
			'width': sorter_data.width,
			'height': sorter_data.height,
			'volume': cal_vol,
			'assigned_dest': chute_id,
			'chuteName': chute_rec.get('chuteName') or self._dest_get(chute_rec, 'chute_name', '') or '',
		}

		self.assign_carrier_to_destination(
			carrier_number=carrier_number,
			dest_identifier=chute_id,
			scanner=getattr(sorter_data, 'scanner', None),
			transit_info=issue_info,
			assigned_name=issue_info.get('chuteName'),
			extra_carrier_updates={'recirculation_count': 1},
			extra_dest_updates=dict(dest_updates_common, chute_info=dest_updates_chute),
		)

		self.log_event('Routing', reason='Found chute for ibn: {}'.format(issue_id), code=3, ibn=issue_id, destination='')
		self.log_event('Routing', reason='Routing {} on carrier: {} to chute:{}'.format(issue_id, carrier_number, chute_id), ibn=issue_id, destination=chute_id, code=4)

		return chute_id

	def _get_next_available_chute(self, sorter_data, group_id, zone):
		sort_by = int(self.get_permissive('sort_by') or 0)

		try:
			last_used = str(system.tag.readBlocking(['[EuroSort]EuroSort/Level3/Control/Last_Used'])[0].value)
		except Exception:
			last_used = 'A'

		if sort_by == 1:
			for _ in range(2):
				search = {
					"A": {"$regex": "^C[0-9]{6}A$"},
					"B": {"$regex": "^C[0-9]{6}B$"},
				}
				side = 'B' if last_used == 'A' else 'A'
				filt = {"$and": [
					{"chuteName": search.get(side)},
					{"occupied": False},
					{"in_service": True},
					{"queued": False},
					{"faulted": False},
					{"wcs_processed": True},
				]}
				chutes = self._find_destinations(filt)
				if not chutes:
					last_used = side
					continue

				if int(chutes[0].get('lane', 0) or 0) in (3, 4):
					chutes = chutes[::-1]

				dest = self._process_chute_result(chutes[0], sorter_data, group_id, zone)
				if dest:
					self._safe_tag_write('[EuroSort]EuroSort/Level3/Control/Last_Used', side)
					return dest

				last_used = side

		elif sort_by == 2:
			lanes = [1, 2, 3, 4]
			num_lanes = len(lanes)

			start_idx = (lanes.index(self.last_sorted_lane) + 1) % num_lanes

			for _ in range(2):
				for i in range(num_lanes):
					next_lane = lanes[(start_idx + i) % num_lanes]
					if int(self.get_permissive("lane{}_enabled".format(next_lane)) or 0) != 1:
						continue

					filt = {"$and": [
						{"lane": next_lane},
						{"occupied": False},
						{"in_service": True},
						{"queued": False},
						{"faulted": False},
						{"wcs_processed": True},
					]}

					chutes = self._find_destinations(filt)
					if not chutes:
						continue

					if next_lane in (3, 4):
						chutes = chutes[::-1]

					self.last_sorted_lane = next_lane
					return self._process_chute_result(chutes[0], sorter_data, group_id, zone)

		return None

	# -------------------------------------------------------------------------
	# Verify / discharge handling
	# -------------------------------------------------------------------------

	def handle_verify(self, sorter_data):
		super(Level_3_OrderRouting, self).handle_verify(sorter_data)

		raw_dest = sorter_data.destination or ''
		if raw_dest in NODEST:
			return

		try:
			chute_fields = raw_dest.split('-')
			station = int(chute_fields[2])
			side = chute_fields[4]
			destination = 'DST-{station:04d}-1-1-{side}'.format(station=station, side=side)
		except Exception:
			try:
				destination = Destination.parse(raw_dest).destination
			except Exception:
				return

		chute_info = self.destination_get(destination) or {}
		carrier_number = int(sorter_data.carrier_number)

		carrier_rec = self.carrier_get(carrier_number) or {}
		issue_info = carrier_rec.get('issue_info') or {}

		if not isinstance(issue_info, dict) or not issue_info:
			issue = self._resolve_issue(sorter_data.barcode) or {}
			issue_info = {
				'ibn': str(issue.get('_id') or sorter_data.barcode or ''),
				'zone': str(issue.get('zone') or ''),
				'group_id': str(issue.get('group_id') or ''),
				'length': sorter_data.length,
				'width': sorter_data.width,
				'height': sorter_data.height,
				'volume': float(sorter_data.length or 0) * float(sorter_data.width or 0) * float(sorter_data.height or 0),
			}

		code = sorter_data.message_code

		if code == 18010:
			self.log_event('Routing', reason='Attempting to deliver ibn: {} to chute: {}'.format(issue_info.get('ibn'), destination), ibn=issue_info.get('ibn'), destination=destination, code=6)
			self.mark_carrier_attempted(carrier_number)
			return

		elif code == MessageCode.ITEM_DISCHARGED_AT_WRONG_DESTINATION:
			self.log_event('Routing', reason='Ibn:{} was delivered to {} from carrier:{}'.format(issue_info.get('ibn'), destination, carrier_number), ibn=issue_info.get('ibn'), destination=destination, code=42)
			self.divert_counts['diverts']['Failed_Wrong'] += 1
			self.mark_carrier_failed(carrier_number)

		elif code == MessageCode.DISCHARGE_ABORTED_POSITIONING_ERROR:
			self.log_event('Routing', reason='Ibn:{} was aborted due to positioning error on carrier:{}'.format(issue_info.get('ibn'), carrier_number), ibn=issue_info.get('ibn'), destination=destination, code=42)
			self.divert_counts['diverts']['Failed_Wrong'] += 1
			self.mark_carrier_aborted(carrier_number)

		elif code == MessageCode.DISCHARGE_ABORTED_DESTINATION_FULL:
			self.log_event('Routing', reason='{}: reached chute full sensor'.format(destination), ibn='', destination=destination, code=12)
			self.divert_counts['diverts']['Failed_Full'] += 1

			rec = self.carrier_get(carrier_number) or {}
			rc = int(rec.get('recirculation_count', 0) or 0) + 1
			self.carrier_update(carrier_number, {'recirculation_count': rc})

			self.log_event('Routing', reason='Failed to deliver ibn:{} to chute{}'.format(issue_info.get('ibn'), destination), ibn=issue_info.get('ibn'), destination=destination, code=7)
			self.mark_carrier_failed(carrier_number)

		elif code == MessageCode.DISCHARGED_AT_DESTINATION:
			zone = str(issue_info.get('zone') or '')
			if zone == 'NoRead':
				self.divert_counts['diverts']['Confirmed_NoRead'] += 1
			elif zone == 'Jackpot':
				self.divert_counts['diverts']['Confirmed_Jackpot'] += 1
			else:
				self.divert_counts['diverts']['Confirmed'] += 1

			self._finalize_discharge(destination, chute_info, issue_info, code)
			self.log_event('Routing', reason='Delivered ibn: {} to chute: {}'.format(issue_info.get('ibn'), destination), ibn=issue_info.get('ibn'), destination=destination, code=8)
			self.mark_carrier_delivered(carrier_number)

		else:
			if code not in [18010, 18011, 18013, 18005, 18026, 18004]:
				self.log_event('Routing', reason='ibn:{} to chute: {} from carrier:{} for code: {}'.format(issue_info.get('ibn'), destination, carrier_number, code), ibn=issue_info.get('ibn'), destination=destination, code=99)
				self.divert_counts['diverts']['Failed_Other'] += 1
				self.mark_carrier_unknown(carrier_number)

		total = (
			self.divert_counts['diverts']['Confirmed_NoRead']
			+ self.divert_counts['diverts']['Confirmed_Jackpot']
			+ self.divert_counts['diverts']['Confirmed']
			+ self.divert_counts['diverts']['Failed_Wrong']
		)
		self.divert_counts['diverts']['Total_Discharged'] = total
		self._safe_tag_write('[EuroSort]EuroSort/Level3/Sorter_Control/Status/divert_Counts', self.divert_counts)

	def _finalize_discharge(self, chute_id, chute_info, issue_info, message_code):
		chute_actual = chute_info or {}

		if not bool(chute_actual.get('occupied', False)):
			self.log_event('Routing', reason='{}: has been set to occupied'.format(chute_id), ibn='', destination=chute_id, code=29)

		original_vol = float(self._dest_get(chute_actual, 'volume', 0.0) or 0.0)
		extra_pct = float(self.get_permissive('extra_volume') or 0.0)

		issue_vol = float(issue_info.get('volume', 0.0) or 0.0)
		issue_vol = issue_vol + (issue_vol * extra_pct / 100.0)

		new_volume, percent_full = self._calculate_volume_metrics(original_vol, issue_vol)
		tote_full = True if float(percent_full) >= float(self.get_permissive('max_fill') or 0.0) else False

		if tote_full:
			self.log_event('Routing', reason='{}: reached tote full volume percentage'.format(chute_id), ibn='', destination=chute_id, code=11)

		ibns = self._parse_list_field(self._dest_get(chute_actual, 'ibns', ''))
		zones = self._parse_list_field(self._dest_get(chute_actual, 'zone', ''))
		groups = self._parse_list_field(self._dest_get(chute_actual, 'group_id', ''))

		ibn = str(issue_info.get('ibn') or '')
		zone = str(issue_info.get('zone') or '')
		gid = str(issue_info.get('group_id') or '')

		if ibn and ibn not in ibns:
			ibns.append(ibn)
		if zone and zone not in zones:
			zones.append(zone)
		if gid and gid not in groups:
			groups.append(gid)

		self._dest_update(
			chute_id,
			common_updates={
				'_id': chute_id,
				'occupied': True,
			},
			chute_updates={
				'volume': round(float(new_volume), 2),
				'volume_percent_full': round(float(percent_full), 2),
				'toteFull': bool(tote_full),
				'occupied': True,
				'zone': ','.join(zones),
				'group_id': ','.join(groups),
				'ibns': ','.join(ibns),
				'chuteCount': int(self._dest_get(chute_actual, 'chuteCount', 0) or 0) + 1,
				'wcs_processed': False,
				'waiting_for_processing': False,
			}
		)

		self._safe_tag_write([
			'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Occupied'.format(chute_id),
			'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/WCS_Processed'.format(chute_id),
			'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Waiting_For_Processing'.format(chute_id),
		], [True, False, False])

		if bool(self.get_permissive('auto_active')) and bool(tote_full) and (zone not in ['Jackpot', 'NoRead']):
			self._safe_tag_write([
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Available'.format(chute_id),
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/Queued'.format(chute_id),
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/ToteFull'.format(chute_id),
			], [True, True, True])
			self.log_event('Routing', reason='{}: requested to be released'.format(chute_id), ibn='', destination=chute_id, code=13)

		elif bool(self.get_permissive('auto_active')) and bool(tote_full) and (zone in ['Jackpot', 'NoRead']):
			self._safe_tag_write(
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/ToteFull'.format(chute_id),
				True
			)

		elif (not bool(self.get_permissive('auto_active'))) and bool(tote_full):
			self._safe_tag_write(
				'[EuroSort]EuroSort/Level3/Destinations/{}/Destination/ToteFull'.format(chute_id),
				True
			)

		if zone != 'NoRead':
			if ibn.lower() == 'noread':
				return

			if not bool(self.get_permissive('squelch_WCS')):
				self.notify_wcs_deliver(issue_info)
				self.log_event('Routing', reason='WCS notified ibn: {} delivered to chute {}'.format(ibn, chute_id), ibn=ibn, destination=chute_id, code=30)
			else:
				self.log_event('Routing', reason='WCS was not notified ibn: {} delivering to chute {}'.format(ibn, chute_id), ibn=ibn, destination=chute_id, code=30)


# ===========================================================================
# LEVEL 2
# ===========================================================================

class Level_2_OrderRouting(
	EuroSorterContentTracking,
	EuroSorterPermissivePolling,
	EuroSorterPolling,
	EuroSorterAccessWCS,
	EuroSorterLightControl,
):

	CONTROL_PERMISSIVE_TAG_MAPPING = {
		'max_noread_recirc': 'No Read recirc attempts',
		'squelch_wcs_updates': 'Squelch WCS',
		'max_resort_recirc': 'recirc attempts',
		'level3_dest': 'Level3_Dest',
		'clearance_height': '%s/clearance_height' % (get_dims),

		'by_max_h': '%s/height' % (get_max),
		'by_max_l': '%s/length' % (get_max),
		'by_max_w': '%s/width' % (get_max),
		'by_max_v': '%s/volume' % (get_max),
		'by_max_all': '%s/all' % (get_max),
		'by_max_any': '%s/any' % (get_max),

		'by_min_h': '%s/height' % (get_min),
		'by_min_l': '%s/length' % (get_min),
		'by_min_w': '%s/width' % (get_min),
		'by_min_v': '%s/volume' % (get_min),
		'by_min_all': '%s/all' % (get_min),
		'by_min_any': '%s/any' % (get_min),

		'max_dims': '%s/max_dims' % (get_dims),
		'min_dims': '%s/min_dims' % (get_dims),
		'tote_dims': '%s/tote_dims' % (get_dims),

		'ratio_long_short_ratio': '%s/ratio_long_short_ratio' % (get_max),
		'tube_ratio_flatness_ratio': '%s/tube_ratio_flatness_ratio' % (get_max),
		'box_ratio_flatness_ratio': '%s/box_ratio_flatness_ratio' % (get_max),
		'aspect_balance_ratio': '%s/aspect_balance_ratio' % (get_max),

		'reset_dict': 'clear_defaults',
		'reload_state': 'Reload Routes',
	}

	def __init__(self, name, **init_cfg):
		super(Level_2_OrderRouting, self).__init__(name, **init_cfg)
		self.logger = Logger(name)
		self.DEST_BASE_PATH = '[EuroSort]EuroSort/%s/Destinations' % name
		self.issue_info = {}
		self._last_check_processed_chutes = system.date.now()
		self.maxjackpot = 0
		self.maxnoread = 0

		self.scanner_id = None
		self.loaded_defaults = False

		self.DEST_STATUS_TAGS = {
			'in_service': 'In_Service',
			'dfs':        'DFS',
			'ofs':        'OFS',
			'faulted':    'Faulted',
			'status':     'Light/Status',
		}

		for perm, tag in self.CONTROL_PERMISSIVE_TAG_MAPPING.items():
			self._subscribe_control_permissive(perm, tag)

		self._polling_methods.append(self._refresh_destination_status_from_tags)

		self._init_polling()

		self.load_default_chutes()

		if self._gp('reset_dict', False):
			self._clear_chutes()

	# -----------------------------------------------------------------
	# Shared destination helpers (new contents.py)
	# -----------------------------------------------------------------

	def _dest_info(self, rec):
		if not isinstance(rec, dict):
			return {}
		info = rec.get('chute_info')
		return info if isinstance(info, dict) else {}

	def _dest_get(self, rec, key, default=None):
		if not isinstance(rec, dict):
			return default

		if key in rec:
			return rec.get(key, default)

		info = rec.get('chute_info')
		if isinstance(info, dict):
			return info.get(key, default)

		return default

	def _dest_update(self, destination, common_updates=None, chute_updates=None):
		common_updates = common_updates or {}
		chute_updates = chute_updates or {}

		current = self.destination_get(destination) or {}
		current_info = current.get('chute_info')
		if not isinstance(current_info, dict):
			current_info = {}

		merged = dict(common_updates)
		if chute_updates:
			merged['chute_info'] = dict(current_info, **chute_updates)

		if 'last_updated' not in merged:
			merged['last_updated'] = system.date.now()

		self.destination_update(destination, merged)

	# ------------------------------ helpers --------------------------------

	def _gp(self, name, default=None):
		try:
			return self.get_permissive(name)
		except Exception:
			return default

	def load_default_chutes(self):
		if not self.loaded_defaults:
			chutes_to_load = {
				'Level3': 'DST-0120-1-1-A',
				'CrossDock': 'DST-0105-1-1-A',
			}
			for key, destination in chutes_to_load.items():
				chute = self.destination_get(destination) or {}

				current_names = self._dest_get(chute, 'assigned_name', []) or []
				if isinstance(current_names, basestring):
					current_names = [current_names]

				is_assigned = bool(self._dest_get(chute, 'assigned', False))

				if key not in current_names:
					current_names.append(key)
				if not is_assigned:
					is_assigned = True

				self._dest_update(
					destination,
					chute_updates={
						'assigned_name': current_names,
						'assigned': is_assigned,
					}
				)
			self.loaded_defaults = True

	def clear_all_destinations(self, reload_defaults=True):
		updated = 0
		ts = system.date.now()

		try:
			cache_key = "SORTER_DESTINATIONS_%s" % self.name
			ExtraGlobal[cache_key] = {}
			self.logger.info("Cleared ExtraGlobal cache key: %s" % cache_key)
		except Exception, e:
			self.logger.warn("Failed clearing ExtraGlobal destination cache: %s" % str(e))

		try:
			destination_names = list(self.destinations_all_transit_info().keys())
		except Exception, e:
			self.logger.error("Unable to get destinations for clear: %s" % str(e))
			destination_names = []

		for destination in destination_names:
			try:
				self._dest_update(
					destination,
					common_updates={
						'enroute': 0,
						'delivered': 0,
						'enqueue': 0,
						'last_updated': ts,
					},
					chute_updates={
						'assigned_name': [],
						'assigned': False,
						'assigned_mode': '',
						'transit_info': {},
						'dfs': False,
						'ofs': False,
						'light_status': 'OFF',
					}
				)
				updated += 1
			except Exception, e:
				self.logger.warn("Failed clearing destination %s: %s" % (destination, str(e)))

		if reload_defaults:
			self.loaded_defaults = False
			self.load_default_chutes()

		return {"ok": True, "data": {"updated": updated}, "message": None}

	def _destination_status_tagpaths(self, dest_key):
		base = self.DEST_BASE_PATH
		prefix = "%s/%s/Destination" % (base, dest_key)
		paths = {}
		for field_name, tag_name in self.DEST_STATUS_TAGS.items():
			paths[field_name] = "%s/%s" % (prefix, tag_name)
		return paths

	def _encode_light_mode_to_tag(self, mode):
		mode = (mode or 'Off').upper()
		if mode == 'OFF':
			return 0
		elif mode == 'ON':
			return 1
		elif mode == 'BLINK1':
			return 2
		elif mode == 'BLINK2':
			return 3
		return 0

	def _set_chute_light_mode(self, dest_key, mode):
		mode = (mode or 'Off').upper()
		if mode not in ('OFF', 'ON', 'BLINK1', 'BLINK2'):
			mode = 'OFF'

		self._dest_update(dest_key, chute_updates={'light_status': mode})

		try:
			tagpaths = self._destination_status_tagpaths(dest_key)
			status_path = tagpaths.get('status')
			if status_path:
				value = self._encode_light_mode_to_tag(mode)
				system.tag.writeBlocking([status_path], [value])
		except Exception:
			logger = getattr(self, 'log', None) or system.util.getLogger('Level_2_OrderRouting')
			logger.warn("Failed to write light status tag for %s to mode %s" % (dest_key, mode))

	def _evaluate_assigned_group_lights(self, changed_dest_key):
		changed_rec = self.destination_get(changed_dest_key) or {}
		assigned_name = self._dest_get(changed_rec, 'assigned_name')
		if not assigned_name:
			return

		def _has_assigned_name(rec, name):
			rec_name = self._dest_get(rec, 'assigned_name')
			if rec_name is None:
				return False
			if isinstance(rec_name, (list, tuple, set)):
				return name in rec_name
			return str(name) in str(rec_name)

		group_keys = []
		for dest_key in self._sorted_destinations():
			rec = self.destination_get(dest_key)
			if rec is None:
				continue

			if not _has_assigned_name(rec, assigned_name):
				continue

			if not rec.get('in_service', True):
				continue
			if self._dest_get(rec, 'dfs', False) or self._dest_get(rec, 'ofs', False) or rec.get('faulted', False):
				continue

			group_keys.append(dest_key)

		if not group_keys:
			return

		full_map = {}
		for dest_key in group_keys:
			rec = self.destination_get(dest_key) or {}
			is_full = bool(self._dest_get(rec, 'dfs', False))
			full_map[dest_key] = is_full

		total = len(group_keys)
		num_full = sum(1 for v in full_map.values() if v)

		if num_full == 0:
			for dest_key in group_keys:
				self._set_chute_light_mode(dest_key, 'OFF')
			return

		if 0 < num_full < total:
			for dest_key, is_full in full_map.items():
				if is_full:
					self._set_chute_light_mode(dest_key, 'ON')
				else:
					self._set_chute_light_mode(dest_key, 'OFF')
			return

		if num_full == total:
			for dest_key in group_keys:
				self._set_chute_light_mode(dest_key, 'BLINK1')
			return

	def _on_destination_status_changed(self, dest_key, changed):
		if ('dfs' in changed or
			'in_service' in changed or
			'ofs' in changed or
			'faulted' in changed):
			self._evaluate_assigned_group_lights(dest_key)

	def _refresh_destination_status_from_tags(self):
		try:
			all_dest = self.destinations_all_transit_info().keys()
		except Exception:
			all_dest = []

		all_dest = list(all_dest) or []
		if not all_dest:
			return

		read_paths = []
		meta = []

		for dest_key in all_dest:
			tagpaths = self._destination_status_tagpaths(dest_key)
			for field_name, path in tagpaths.items():
				if field_name == 'status':
					continue
				read_paths.append(path)
				meta.append((dest_key, field_name))

		if not read_paths:
			return

		results = system.tag.readBlocking(read_paths)

		updates_by_dest = {}
		for (dest_key, field_name), r in zip(meta, results):
			try:
				q = getattr(r, "quality", None)
				if q is not None and not q.isGood():
					continue
				value = bool(r.value)
			except Exception:
				continue

			dest_updates = updates_by_dest.setdefault(dest_key, {})
			dest_updates[field_name] = value

		for dest_key, updates in updates_by_dest.items():
			current = self.destination_get(dest_key) or {}
			changed_common = {}
			changed_chute = {}

			for k, v in updates.items():
				if k in ('dfs', 'ofs'):
					if self._dest_get(current, k) != v:
						changed_chute[k] = v
				else:
					if current.get(k) != v:
						changed_common[k] = v

			if changed_common or changed_chute:
				self._dest_update(dest_key, changed_common, changed_chute)
				merged = {}
				merged.update(changed_common)
				merged.update(changed_chute)
				self._on_destination_status_changed(dest_key, merged)

	def _update_destination_status(self, dest_key, **fields):
		if not fields:
			return

		common_updates = {}
		chute_updates = {}

		for field_name, value in fields.items():
			if field_name in ('dfs', 'ofs'):
				chute_updates[field_name] = value
			else:
				common_updates[field_name] = value

		self._dest_update(dest_key, common_updates, chute_updates)

		tagpaths = self._destination_status_tagpaths(dest_key)
		write_paths = []
		write_values = []

		for field_name, value in fields.items():
			if field_name not in tagpaths:
				continue
			if field_name == 'status':
				continue
			write_paths.append(tagpaths[field_name])
			write_values.append(bool(value))

		if write_paths:
			try:
				system.tag.writeBlocking(write_paths, write_values)
			except Exception:
				logger = getattr(self, 'log', None) or system.util.getLogger('Level_2_OrderRouting')
				logger.warn("Failed to write status tags for %s: %r" % (dest_key, fields))

		self._on_destination_status_changed(dest_key, fields)

	# -------------------------- Level 2 routers ----------------------------

	def _route_order(self):
		assigned_name = (self.issue_info or {}).get('assigned_name')
		assigned_mode = (self.issue_info or {}).get('assigned_mode')
		system.util.getLogger("RouteDecision").info("%s:%s" % (assigned_name, assigned_mode))
		return self._get_chute_location(assigned_name, assigned_mode)

	def _route_to_label(self):
		carrier = self.carrier or {}
		assigned_name = carrier.get('assigned_name')
		return self.get_chute_by_assigned_name(assigned_name)

	def _route_unresolved(self):
		return self._route_to_label()

	def _route_jackpot(self):
		return self._route_to_label()

	def _route_nocode(self):
		return self._route_to_label()

	def _route_noscan(self):
		return self._route_to_label()

	def _route_noread(self):
		max_count = int(self.get_permissive('max_noread_recirc'))
		carrier = self.carrier or {}
		recirc_count = carrier.get('recirculation_count', 0)
		carrier_num = carrier.get('carrier_number', None)
		induct_scanner = carrier.get('induct_scanner', None)

		if self.scanner_id == induct_scanner:
			recirc_count += 1
			if carrier_num is not None:
				self.carrier_update(
					carrier_num,
					recirculation_count=recirc_count
				)

		if recirc_count >= max_count:
			assigned_name = 'NOREAD'
			return self.get_chute_by_assigned_name(assigned_name)

		return None

	def _max_recirc(self):
		max_recirc = int(self.get_permissive('max_resort_recirc'))
		carrier = self.carrier or {}
		recirc_count = carrier.get('recirculation_count', 0)
		carrier_num = carrier.get('carrier_number', None)
		induct_scanner = carrier.get('induct_scanner', None)

		if self.scanner_id == induct_scanner:
			recirc_count += 1
			if carrier_num is not None:
				self.carrier_update(
					carrier_num,
					recirculation_count=recirc_count
				)

		if recirc_count >= max_recirc:
			assigned_name = 'JACKPOT'
			return self.get_chute_by_assigned_name(assigned_name)

		return None

	# ------------------------ chute lookup helpers -------------------------

	def _assigned_name_matches(self, target_name, assigned_value):
		def _canon(v):
			try:
				if v is None:
					return ''
				return str(v).strip().upper()
			except Exception:
				return ''

		def _as_list(v):
			if v is None:
				return []
			if isinstance(v, basestring):
				s = _canon(v)
				return [s] if s else []
			if isinstance(v, (list, tuple, set)):
				out = []
				for x in v:
					s = _canon(x)
					if s:
						out.append(s)
				return out
			s = _canon(v)
			return [s] if s else []

		target_name = _canon(target_name)
		if not target_name:
			return False

		for token in _as_list(assigned_value):
			if token == target_name:
				return True

			m = re.match(r'^\[([A-Z])-([A-Z])\]$', token)
			if m and len(target_name) == 1:
				if m.group(1) <= target_name <= m.group(2):
					return True

		return False

	def get_chute_by_assigned_name(self, assigned_name=None, assigned_mode=None):
		carrier = self.carrier or {}
		fallback_to_jackpot = True

		if not assigned_name:
			assigned_name = carrier.get('assigned_name')

		if assigned_name is None:
			return None

		def _canon(v):
			try:
				if v is None:
					return ''
				return str(v).strip().upper()
			except Exception:
				return ''

		assigned_name = _canon(assigned_name)
		assigned_mode = _canon(assigned_mode or carrier.get('assigned_mode'))
		issue_info = self.issue_info or {}

		is_tote = bool(issue_info.get('is_tote', False))
		oh_clearance = bool(issue_info.get('clearance_over', False))
		missing_dims = self._has_missing_dims(issue_info)

		if is_tote or missing_dims or oh_clearance:
			set_match_pattern = 1
		else:
			set_match_pattern = 3

		chute_patterns = {
			1: r"^B[0-9]{4}21(A|B)$",
			2: r"^B[0-9]{4}11(A|B)$",
			3: r"^B[0-9]{4}(1|2)1(A|B)$"
		}

		pattern_str = chute_patterns.get(set_match_pattern)
		pattern_re = re.compile(pattern_str) if pattern_str else None

		def _is_special_destination(dest_key, rec):
			rec = rec or {}
			names = self._dest_get(rec, 'assigned_name', []) or []

			if isinstance(names, basestring):
				names = [names]

			names = [str(x).strip().upper() for x in names if x]

			special_names = set(['LEVEL3', 'CROSSDOCK', 'UNRESOLVED'])

			if dest_key in ('DST-0120-1-1-A', 'DST-0105-1-1-A'):
				return True

			for name in names:
				if name in special_names:
					return True

			return False

		def _find_match(target_name):
			target_name = _canon(target_name)
			if not target_name:
				return None

			for dest_key in self._sorted_destinations():
				rec = self.destination_get(dest_key)
				if rec is None:
					continue

				chute_code = self._dest_get(rec, 'chute_name') or dest_key
				if pattern_re and chute_code and not pattern_re.match(chute_code):
					continue

				if not rec.get('in_service', False):
					continue

				if self._dest_get(rec, 'dfs', False) or self._dest_get(rec, 'ofs', False) or rec.get('faulted', False):
					continue

				if not bool(self._dest_get(rec, 'assigned', False)):
					continue

				if target_name not in ('JACKPOT', 'NOREAD', 'LEVEL3', 'CROSSDOCK', 'UNRESOLVED'):
					if _is_special_destination(dest_key, rec):
						continue

				if not self._assigned_name_matches(target_name, self._dest_get(rec, 'assigned_name')):
					continue

				return dest_key

			return None

		dest = _find_match(assigned_name)
		if dest is not None:
			return dest

		if assigned_mode == 'PRE':
			first_letter = assigned_name[:1]
			if first_letter:
				dest = _find_match(first_letter)
				if dest is not None:
					return dest

		if fallback_to_jackpot:
			return _find_match('JACKPOT')

		return None

	def _validate_destination(self, destination):
		chute = self.destination_get(destination)
		if chute is None:
			return None

		if not chute.get('in_service', True):
			return None

		if self._dest_get(chute, 'dfs', False) or self._dest_get(chute, 'ofs', False) or chute.get('faulted', False):
			return None

		return destination

	def _get_carrierinfo(self, carrier_num):
		self.carrier = self.carrier_get(carrier_num)
		if self.carrier is None:
			return None

		destination = self.carrier.get('destination')
		if not destination:
			return None

		self.issue_info = self.carrier.get('issue_info', {}) or {}

		self.logger.info('%s: %s' % (carrier_num, destination))

		if self.issue_info:
			recirc_count = self.carrier.get('recirculation_count', 0)
			if self.scanner_id == self.carrier.get('induct_scanner', None):
				recirc_count += 1

			if self.carrier.get('discharged_attempted', False):
				self.carrier_update(
					carrier_num,
					discharged_attempted=False,
					destination=''
				)

			if destination:
				destination = self._validate_destination(destination)

			return destination

		return None

	def get_carrier_update_info(self, carrier_number):
		rec = self.carrier_get(carrier_number)
		if rec is None:
			return None, None
		dest = rec.get('destination', None)
		track_id = rec.get('track_id', None)
		if dest:
			dest = Destination.parse(dest)
		if not track_id:
			track_id = None
		return dest, track_id

	def get_carrier_destination(self, carrier_number):
		rec = self.carrier_get(carrier_number)
		if rec is None:
			return None
		dest = rec.get('destination', None)
		if not dest:
			return None
		return dest

	def get_carrier_issue(self, carrier_number):
		rec = self.carrier_get(carrier_number)
		if rec is None:
			return None
		issue_info = rec.get('issue_info', None)
		if not issue_info:
			return {}
		return issue_info

	# ------------------------ main Level 2 routing -------------------------

	def route_destination(self, sorter_data):
		carrier_num = sorter_data.carrier_number
		track_id = sorter_data.track_id

		self.scanner_id = sorter_data.station_id
		if carrier_num in (None, '', 'None'):
			self.logger.error(
				"route_destination missing carrier_number; station_id=%r track_id=%r barcodes=%r sorter_data=%r"
				% (
					getattr(sorter_data, 'station_id', None),
					getattr(sorter_data, 'track_id', None),
					getattr(sorter_data, 'barcodes', None),
					sorter_data
				)
			)
			return None

		self.carrier = self.carrier_get(carrier_num) or {}

		self.carrier_update(
			carrier_num,
			induct_scanner=self.scanner_id,
			track_id=track_id
		)

		system.util.getLogger('dims').info('dims are :' + str(sorter_data.dimensions))

		prev_dest = self.carrier.get('destination', None)
		prev_assigned_name = self.carrier.get('assigned_name', None)
		prev_assigned_mode = self.carrier.get('assigned_mode', None)

		self.issue_info = self.carrier.get('issue_info', {}) or {}
		self.issue_info.update({'induct_scanner': self.scanner_id})

		if prev_dest and prev_assigned_name:
			if self._validate_destination(prev_dest):
				return prev_dest

			destination = self.get_chute_by_assigned_name(prev_assigned_name, prev_assigned_mode)
			if destination:
				return destination

		code, assigned_name, assigned_mode, router = self.define_and_detect(
			sorter_data.barcodes
		)

		detected_is_tote = bool((self.issue_info or {}).get('is_tote', False))
		induct_scanner = (self.issue_info or {}).get('induct_scanner')

		self.issue_info = {
			'barcode':        code,
			'assigned_name':  assigned_name,
			'assigned_mode':  assigned_mode,
			'router':         router,
			'is_tote':        detected_is_tote,
			'induct_scanner': induct_scanner
		}

		self.calculate_product_dims(
			sorter_data.length,
			sorter_data.width,
			sorter_data.height
		)

		if not self.issue_info.get('is_tote', False):
			self.determine_product_size()

		self.carrier_update(
			carrier_num,
			issue_info=self.issue_info,
			assigned_name=assigned_name,
			assigned_mode=assigned_mode
		)

		destination = None

		try:
			if router in ["NOREAD", "NOCODE", "NOSCAN", "JACKPOT", "UNRESOLVED", "SDR"]:
				destination = self.get_chute_by_assigned_name(assigned_name, assigned_mode)

			elif router == 'DST':
				destination = code

			else:
				destination = self.get_chute_by_assigned_name(assigned_name, assigned_mode)
				self.logger.info(
					"attempting route barcode=%s assigned_name=%s assigned_mode=%s is_tote=%s destination=%s"
					% (
						code,
						assigned_name,
						assigned_mode,
						self.issue_info.get('is_tote', False),
						destination
					)
				)

			if destination is not None:
				self.assign_carrier_to_destination(
					carrier_num,
					destination,
					track_id=track_id,
					scanner=self.scanner_id,
					transit_info=self.issue_info
				)
				self.logger.info('attempting to route to %s' % destination)
				return destination

			destination = self.get_chute_by_assigned_name(assigned_name, assigned_mode)
			if destination is not None:
				self.assign_carrier_to_destination(
					carrier_num,
					destination,
					track_id=track_id,
					scanner=self.scanner_id,
					transit_info=self.issue_info
				)
				self.logger.info('attempting fallback route to %s' % destination)
				return destination

		except Exception:
			self.logger.warn(
				"Level_2_OrderRouting.route_destination error: %s"
				% python_full_stack()
			)
			return None

		return None

	# ----------------------- size / dims helpers ---------------------------

	def _has_missing_dims(self, issue_info=None):
		issue_info = issue_info or self.issue_info or {}

		try:
			l = _to_float(issue_info.get('length'))
			w = _to_float(issue_info.get('width'))
			h = _to_float(issue_info.get('height'))
		except Exception:
			return True

		if l <= 0 or w <= 0 or h <= 0:
			return True

		return False

	def calculate_product_dims(self, l, w, h):
		tote_dims = self._gp('tote_dims', {}) or {}
		is_tote = self.issue_info.get('is_tote', False)

		if is_tote:
			l = round(_to_float(tote_dims.get('length')), 2)
			w = round(_to_float(tote_dims.get('width')), 2)
			h = round(_to_float(tote_dims.get('height')), 2)
			self.issue_info.update({
				'size_reason':    ['Found Tote'],
				'shape':          'Tote',
				'clearance_over': True,
			})
		else:
			l = round(_to_float(l), 2)
			w = round(_to_float(w), 2)
			h = round(_to_float(h), 2)

		volume = round(_volume(l, w, h), 2)

		self.issue_info.update({
			'length': l,
			'width':  w,
			'height': h,
			'volume': volume
		})

	def define_and_detect(self, barcodes):
		dst = None
		tote = None
		sdr = None
		ibns = []

		error_seen = {
			'NOREAD': False,
			'NOSCAN': False,
			'NOCODE': False,
		}

		for raw in barcodes:
			code = raw
			if not code:
				continue

			for patt, err_label in error_matches.items():
				if patt.match(code):
					if err_label in error_seen:
						error_seen[err_label] = True
					break
			else:
				for patt, label in code_matches.items():
					if patt.match(code):
						if label == "DST":
							dst = code
						elif label == "TOTE" and tote is None:
							tote = code
							ibns = None
							self.issue_info['is_tote'] = True
						elif label == "SDR" and sdr is None:
							sdr = code
						elif label == "IBN":
							ibns.append(code)
						break

		if dst:
			self.issue_info['codes'] = dst
			first_code = dst
			assigned_name = 'DST'
			assigned_mode = 'Destination'
			router = 'DST'
			return first_code, assigned_name, assigned_mode, router

		if sdr:
			self.issue_info['codes'] = sdr
			first_code = sdr
			assigned_name = 'SDR'
			assigned_mode = 'SDR'
			router = 'SDR'
			return first_code, assigned_name, assigned_mode, router

		lookup_codes = []
		if tote:
			lookup_codes.append(tote)
		if ibns and not tote:
			lookup_codes.extend(ibns)

		if lookup_codes:
			first_code, assigned_name, assigned_mode, router = self.wcs_lookup(lookup_codes)

			if self.issue_info.get('inspect', False):
				self.issue_info['size_mode'] = '%s-inspection' % self.issue_info['shape']

			return first_code, assigned_name, assigned_mode, router

		if error_seen['NOREAD']:
			return 'NOREAD', 'NOREAD', 'NOREAD', 'NOREAD'
		if error_seen['NOCODE']:
			return 'NOCODE', 'NOCODE', 'NOCODE', 'NOCODE'
		if error_seen['NOSCAN']:
			return 'NOSCAN', 'NOSCAN', 'NOSCAN', 'NOSCAN'

		return 'UNRESOLVED', 'UNRESOLVED', 'UNRESOLVED', 'UNRESOLVED'

	def _is_special_destination(self, dest_key, rec=None):
		rec = rec or {}
		names = self._dest_get(rec, 'assigned_name', []) or []

		if isinstance(names, basestring):
			names = [names]

		names = [str(x).strip().upper() for x in names if x]

		special_names = set(['LEVEL3', 'CROSSDOCK', 'JACKPOT', 'NOREAD', 'UNRESOLVED'])

		if dest_key in ('DST-0120-1-1-A', 'DST-0105-1-1-A'):
			return True

		for name in names:
			if name in special_names:
				return True

		return False

	def determine_product_size(self):
		size_reason = []
		oversized = False
		undersized = False

		max_dims = self._gp('max_dims', {}) or {}
		min_dims = self._gp('min_dims', {}) or {}

		flags = {
			'min_l':   bool(self._gp('by_min_l', False)),
			'min_w':   bool(self._gp('by_min_w', False)),
			'min_h':   bool(self._gp('by_min_h', False)),
			'min_v':   bool(self._gp('by_min_v', False)),
			'min_any': bool(self._gp('by_min_any', False)),
			'min_all': bool(self._gp('by_min_all', False)),

			'max_l':   bool(self._gp('by_max_l', False)),
			'max_w':   bool(self._gp('by_max_w', False)),
			'max_h':   bool(self._gp('by_max_h', False)),
			'max_v':   bool(self._gp('by_max_v', False)),
			'max_any': bool(self._gp('by_max_any', False)),
			'max_all': bool(self._gp('by_max_all', False)),
		}

		thr = {
			'min_l': _to_float(min_dims.get('length', 0.0)),
			'min_w': _to_float(min_dims.get('width', 0.0)),
			'min_h': _to_float(min_dims.get('height', 0.0)),
			'min_v': _to_float(min_dims.get('volume', 0.0)),

			'max_l': _to_float(max_dims.get('length', 0.0)),
			'max_w': _to_float(max_dims.get('width', 0.0)),
			'max_h': _to_float(max_dims.get('height', 0.0)),
			'max_v': _to_float(max_dims.get('volume', 0.0)),

			'clr_h': _to_float(self._gp('clearance_height', 12.0)),
		}

		l = _to_float(self.issue_info.get('length'))
		w = _to_float(self.issue_info.get('width'))
		h = _to_float(self.issue_info.get('height'))
		v = _to_float(self.issue_info.get('volume'))

		min_checks = []
		if flags['min_l']:
			min_checks.append(('length<min_length', l < thr['min_l']))
		if flags['min_w']:
			min_checks.append(('width<min_width', w < thr['min_w']))
		if flags['min_h']:
			min_checks.append(('height<min_height', h < thr['min_h']))
		if flags['min_v']:
			min_checks.append(('volume<min_volume', v < thr['min_v']))

		max_checks = []
		if flags['max_l']:
			max_checks.append(('length>max_length', l > thr['max_l']))
		if flags['max_w']:
			max_checks.append(('width>max_width', w > thr['max_w']))
		if flags['max_h']:
			max_checks.append(('height>max_height', h > thr['max_h']))
		if flags['max_v']:
			max_checks.append(('volume>max_volume', v > thr['max_v']))

		if flags['min_all']:
			if min_checks and all(result for _, result in min_checks):
				size_reason.append('Undersized')
				undersized = True
		elif flags['min_any']:
			if any(result for _, result in min_checks):
				size_reason.append('Undersized')
				undersized = True
		else:
			for reason, result in min_checks:
				if result:
					size_reason.append(reason)
					undersized = True

		if flags['max_all']:
			if max_checks and all(result for _, result in max_checks):
				size_reason.append('Oversized')
				oversized = True
		elif flags['max_any']:
			if any(result for _, result in max_checks):
				size_reason.append('Oversized')
				oversized = True
		else:
			for reason, result in max_checks:
				if result:
					size_reason.append(reason)
					oversized = True

		clearance_over = (h > thr['clr_h'])

		dims = [l, w, h]
		longest = max(dims)
		shortest = min(dims) if longest > 0 else 0.0
		mid = sorted(dims)[1]

		ratio_long_short = (longest / shortest) if shortest > 0 else 0.0
		ratio_flatness = (h / mid) if mid > 0 else 0.0

		rls_thresh = _to_float(self._gp('ratio_long_short_ratio', 3.0))
		tube_flat_thresh = _to_float(self._gp('tube_ratio_flatness_ratio', 0.5))
		box_flat_thresh = _to_float(self._gp('box_ratio_flatness_ratio', 0.8))

		if ratio_long_short >= rls_thresh and ratio_flatness < tube_flat_thresh:
			shape = 'tube'
		elif ratio_flatness >= box_flat_thresh:
			shape = 'box'
		else:
			shape = 'bag'

		size_mode = ''
		if oversized:
			size_mode = 'Oversized-%s' % shape
		if undersized:
			size_mode = 'Undersized-%s' % shape

		self.issue_info.update({
			'length':         l,
			'width':          w,
			'height':         h,
			'volume':         v,
			'undersized':     undersized,
			'oversized':      oversized,
			'size_reason':    size_reason,
			'shape':          shape,
			'clearance_over': clearance_over,
			'size_mode':      size_mode
		})

	# -------------------- destination sorting helper -----------------------

	def _sorted_destinations(self):
		def sort_key(dest_key):
			try:
				d = Destination.parse(dest_key)
				return (int(d.station), int(d.chute.value), int(d.dest), d.side.value)
			except Exception:
				return (9999, 9, 9999, dest_key)

		return sorted(self.destinations_all_transit_info().keys(), key=sort_key)

	def _get_chute_location(self, assigned_name=None, assigned_mode=None):
		issue_info = self.issue_info or {}
		fallback_to_jackpot = True

		def _canon(v):
			try:
				if v is None:
					return ''
				return str(v).strip().upper()
			except Exception:
				return ''

		if assigned_name is None:
			assigned_name = issue_info.get('assigned_name')

		assigned_name = _canon(assigned_name)

		if assigned_mode is None:
			assigned_mode = issue_info.get('assigned_mode', 'POST')
		assigned_mode = _canon(assigned_mode)

		is_tote = bool(issue_info.get('is_tote', False))
		clearance_over = bool(issue_info.get('clearance_over', False))
		missing_dims = self._has_missing_dims(issue_info)

		if is_tote or missing_dims or clearance_over:
			set_match_pattern = 1
		else:
			set_match_pattern = 3

		chute_patterns = {
			1: r"^B[0-9]{4}21(A|B)$",
			2: r"^B[0-9]{4}11(A|B)$",
			3: r"^B[0-9]{4}(1|2)1(A|B)$"
		}

		pattern_str = chute_patterns.get(set_match_pattern)
		pattern_re = re.compile(pattern_str) if pattern_str else None

		if (not issue_info.get('inspect', False)) and issue_info.get('undersized', False):
			level3_dest = self.get_permissive('level3_dest')
			if level3_dest:
				return level3_dest

		building_id = issue_info.get('building_id')

		def _is_special_destination(dest_key, rec):
			rec = rec or {}
			names = self._dest_get(rec, 'assigned_name', []) or []

			if isinstance(names, basestring):
				names = [names]

			names = [str(x).strip().upper() for x in names if x]

			special_names = set(['LEVEL3', 'CROSSDOCK', 'UNRESOLVED'])

			if dest_key in ('DST-0120-1-1-A', 'DST-0105-1-1-A'):
				return True

			for name in names:
				if name in special_names:
					return True

			return False

		def _find_match(target_name):
			target_name = _canon(target_name)
			if not target_name:
				return None

			for dest_key in self._sorted_destinations():
				rec = self.destination_get(dest_key)
				if rec is None:
					continue

				chute_code = self._dest_get(rec, 'chute_name') or dest_key
				if pattern_re and chute_code and not pattern_re.match(chute_code):
					continue

				if not rec.get('in_service', True):
					continue

				if self._dest_get(rec, 'dfs', False) or self._dest_get(rec, 'ofs', False) or rec.get('faulted', False):
					continue

				if not bool(self._dest_get(rec, 'assigned', False)):
					continue

				if assigned_mode == 'POST':
					if building_id and self._dest_get(rec, 'building_id') != building_id:
						continue

				if target_name not in ('JACKPOT', 'NOREAD', 'LEVEL3', 'CROSSDOCK', 'UNRESOLVED'):
					if _is_special_destination(dest_key, rec):
						continue

				if not self._assigned_name_matches(target_name, self._dest_get(rec, 'assigned_name')):
					continue

				return dest_key

			return None

		dest = _find_match(assigned_name)
		if dest is not None:
			return dest

		if assigned_mode == 'PRE':
			first_letter = assigned_name[:1]
			if first_letter:
				dest = _find_match(first_letter)
				if dest is not None:
					return dest

		if fallback_to_jackpot:
			return self.get_chute_by_assigned_name('JACKPOT')

		return None

	def _process_chute_result(self, chute_name):
		rec = self.destination_get(chute_name)
		if rec is None:
			return chute_name

		transit_info = self._dest_get(rec, 'transit_info', {}) or {}
		transit_info['lastUpdated'] = datetime.now()

		self._dest_update(chute_name, chute_updates={'transit_info': transit_info})
		return chute_name

	# -------------------------- verify handling ----------------------------

	def handle_verify(self, sorter_data):
		super(Level_2_OrderRouting, self).handle_verify(sorter_data)

		raw_dest = sorter_data.destination
		if not raw_dest:
			return

		chute_fields = raw_dest.split('-')
		if len(chute_fields) < 5:
			return

		destination = 'DST-{station:04d}-{chute}-1-{side}'.format(
			station=int(chute_fields[2]),
			chute=chute_fields[3],
			side=chute_fields[4]
		)

		carrier_num = sorter_data.carrier_number

		self.issue_info = self.get_carrier_issue(carrier_num) or {}
		self.logger.info('%s:%s' % (carrier_num, self.issue_info))

		message = sorter_data.message_code
		self.logger.info('%s:%s....type:%s' % (sorter_data.message_code, message, type(message)))
		rec = self.carrier_get(carrier_num)
		discharged_attempted = rec.get('discharged_attempted', False) if rec else False

		if message == MessageCode.DISCHARGE_ATTEMPTED:
			if not discharged_attempted:
				self.mark_carrier_attempted(carrier_num)

		if message == MessageCode.DISCHARGED_AT_DESTINATION:
			self.mark_carrier_delivered(carrier_num)

		elif message == MessageCode.DISCHARGE_FAILED:
			self.mark_carrier_failed(carrier_num)

		elif message == MessageCode.DISCHARGE_ABORTED_DESTINATION_FULL:
			self.mark_carrier_aborted(carrier_num)

		else:
			self.mark_carrier_unknown(carrier_num)