from shared.tools.logging import Logger; Logger().trace('Compiling module')

from shared.data.types.enum import Enum
from shared.tools.global import ExtraGlobal

from eurosort.context import EuroSorterContextManagement
from eurosort.destmap import EuroSorterDestinationMapping
from eurosort.routing import EuroSorterRoutingManagement
from eurosort.sorterdata.destination import SorterDataDestination_DefaultPattern

from datetime import datetime

from database.mongodb.records import select_record, upsert_record

import json, os
import system
import copy


MONGODB    = 'MongoWCS'
MONGO_COLL = 'eurosort_data'


# ---------------------------------------------------------------------------
# Shared chute schema for all sorters
# ---------------------------------------------------------------------------

COMMON_CHUTE_DEFAULT = {
	'_id': None,                     # DST-XXXX-X-X-A/B
	'destination': '',               # same as _id
	'chuteName': '',                 # WCS chute name
	'sorter': '',

	# common chute configuration / state
	'faulted': False,
	'in_service': True,
	'position': None,                # FRONT | REAR | None
	'chute_type': 'NORMAL',          # NORMAL | PACKOUT | HP | JACKPOT | INSPECTION | NOREAD | BAGGING | PURGE | OB
	'lane': 0,
	'occupied': False,
	'available': True,
	'dfs': False,
	'ofs': False,
	'first_item_delivered_ts': None,

	# sorter-specific payload lives here
	'chute_info': {},

	# common tracking / metrics
	'enroute': 0,
	'delivered': 0,
	'last_updated': None,
}


# ---------------------------------------------------------------------------
# Per-sorter chute_info defaults
# ---------------------------------------------------------------------------

LEVEL2_CHUTE_DEFAULT = {
	'building_id': None,
	'ibns': '',
	'oversized': False,
	'undersized': False,
	'shape': None,
	'inspection': False,
	'size_mode': None,
	'assigned': False,
	'assigned_name': [],
	'assigned_mode': '',
	'has_upper_lower': True,
	'has_front_rear': False,
	'has_gate': True,
	'transit_info': {},
}

LEVEL3_CHUTE_DEFAULT = {
	'chuteCount': 0,
	'wcs_processed': True,
	'toteFull': False,
	'queued': False,
	'volume_percent_full': 0.0,
	'waiting_for_processing': False,
	'ibns': '',
	'volume': 0.0,
	'group_id': '',
	'zone': '',
	'chuteFull': False,
	'has_upper_lower': True,
	'has_front_rear': True,
	'has_gate': False,
}



LEVEL3_SHIP_CHUTE_DEFAULT = {

	# [ALL] Order and IBN content
	'orders': [],       # order dicts assigned to this chute/position
	'ibns': [],         # IBN strings physically present

	'sort_codes': [],

	'order_count_total': 0,
	'item_count_total': 0,
	'line_count_total': 0,
	'expected_line_count': 0,            # total lines expected across all assigned orders
	'missing_ibns': [],                  # IBNs assigned but not yet physically discharged
	'percent_orders_consolidated': 0.0,

	'oldest_order_age_sec': 0,

	'contains_priority_order': False,

	# UC9.9 — set True by _finalize_discharge when all items for all orders in this
	# position are consolidated. Triggers the position light via WCS.
	'ready_for_packout': False,

	# UC9.8 — batch door state. DOWN at rest; only raised when utilization threshold exceeded.
	# OB chutes (UC2.1) have no batch door — present but must not be read/written for chute_type == 'OB'.
	'batch_door_state': 'DOWN',          # DOWN | UP | UNKNOWN

	# UC9.8 — rear drop sequencing. Same OB caveat as batch_door_state.
	'rear_drop_pending': False,
	'rear_drop_complete': False,

	'has_upper_lower': True,
	'has_front_rear': True,             # False for OB/JACKPOT/NOREAD/PURGE/INSPECTION/BAGGING
	'has_gate': True,                   # False for same set
}


SORTER_CONFIG = {
	'Level2': {
		'aliases': ('Level2', 'level2', 'LEVEL2'),
		'carrier_max': 772,
		'wcs_prefix': 'B',
		'mode': 'level2',
		'chute_default': LEVEL2_CHUTE_DEFAULT,
		'tag_field_map': {
			'in_service':    'in_service',
			'faulted':       'faulted',
			'chute_type':    'chute_type',
			'lane':          'lane',
			'occupied':      'occupied',
			'available':     'available',
			'dfs':           'dfs',
			'ofs':           'ofs',
			'assigned':      'assigned',
			'assigned_mode': 'assigned_mode',
			'assigned_name': 'assigned_name',
			'oversized':     'oversized',
			'size_mode':     'size_mode',
			'enroute':       'enroute',
			'delivered':     'delivered',
		},
	},
	'Level3': {
		'aliases': ('Level3', 'level3', 'LEVEL3'),
		'carrier_max': 499,
		'wcs_prefix': 'C',
		'mode': 'level3',
		'chute_default': LEVEL3_CHUTE_DEFAULT,
		'tag_field_map': {
			'in_service':             'in_service',
			'faulted':                'faulted',
			'chute_type':             'chute_type',
			'lane':                   'lane',
			'occupied':               'occupied',
			'available':              'available',
			'dfs':                    'dfs',
			'ofs':                    'ofs',
			'zone':                   'zone',
			'group_id':               'group_ID',
			'ibns':                   'ibns',
			'queued':                 'queued',
			'chuteFull':              'chuteFull',
			'toteFull':               'toteFull',
			'wcs_processed':          'wcs_processed',
			'waiting_for_processing': 'waiting_for_processing',
			'volume':                 'volume',
			'volume_percent_full':    'volume_percent_full',
			'chuteCount':             'chuteCount',
			'enroute':                'enroute',
			'delivered':              'delivered',
		},
	},
	'Level3_Ship': {
		'aliases': ('Level3_Ship', 'level3_ship', 'LEVEL3_SHIP', 'Level3Ship', 'level3ship'),
		'wcs_prefix': 'D',
		'mode': 'level3_ship',
		'chute_default': LEVEL3_SHIP_CHUTE_DEFAULT,
		'tag_field_map': {
			'in_service':                  'in_service',
			'faulted':                     'faulted',
			'chute_type':                  'chute_type',     # UI needs this to tell OB from consolidation
			'lane':                        'lane',
			'occupied':                    'occupied',
			'available':                   'available',
			'dfs':                         'dfs',
			'ofs':                         'ofs',
			'first_item_delivered_ts':     'first_item_delivered_ts',
			'has_front_rear':              'has_front_rear', # written so UI can read without calling Python
			'has_gate':                    'has_gate',
			'ready_for_packout':           'ready_for_packout',
			'missing_ibns':                'missing_ibns',
			'expected_line_count':         'expected_line_count',
			'order_count_total':           'order_count_total',
			'item_count_total':            'item_count_total',
			'line_count_total':            'line_count_total',
			'percent_orders_consolidated': 'percent_orders_consolidated',
			'oldest_order_age_sec':        'oldest_order_age_sec',
			'batch_door_state':            'batch_door_state',
			'rear_drop_pending':           'rear_drop_pending',
			'rear_drop_complete':          'rear_drop_complete',
			'sort_codes':                  'sort_codes',
			'contains_priority_order':     'contains_priority_order',
			'orders':                      'orders',
			'ibns':                        'ibns',
			'enroute':                     'enroute',
			'delivered':                   'delivered',
		},
	},
}


# ---------------------------------------------------------------------------
# Operator-selectable vs system-assigned chute types (UC8.1)
# set_chute_type() (Andrew) must reject system-assigned types.
# ---------------------------------------------------------------------------
OPERATOR_SELECTABLE_CHUTE_TYPES = frozenset([
	'NORMAL', 'HP', 'JACKPOT', 'INSPECTION', 'PURGE',
])

SYSTEM_ASSIGNED_CHUTE_TYPES = frozenset([
	'NOREAD',   # system alias for JACKPOT behavior
	'BAGGING',  # re-induction lane, set by destination mapping
	'OB',       # overflow buffer, set by destination mapping
])


class Chutes(Enum):
	LOWER = '1'
	UPPER = '2'


class Dests(Enum):
	REAR = '1'
	FRONT = '2'


class Sides(Enum):
	A = 'A'
	B = 'B'


class Destination(object):
	"""Normalizes and coerces to a standard pattern."""
	__slots__ = ['_station', '_chute', '_side', '_dest', '_context']
	LOOKUP_PROPERTIES = ['station', 'chute', 'dest', 'side']

	def __init__(self, station, chute, dest=None, side=None, **context):
		if side is None and dest is not None:
			side = dest
			dest = None

		if dest is None:
			dest = Dests.REAR

		self._station = self._coerce_station(station)
		self._chute   = self._coerce_chute(chute)
		self._dest    = self._coerce_dest(dest)
		self._side    = self._coerce_side(side)
		self._context = context

	@property
	def station(self): return self._station

	@property
	def chute(self): return self._chute

	@property
	def side(self): return self._side

	@property
	def dest(self): return self._dest

	@property
	def destination(self): return str(self)

	@classmethod
	def _coerce_station(cls, station):
		return '%04d' % int(station)

	@classmethod
	def _coerce_chute(cls, chute):
		if isinstance(chute, Chutes):
			return chute
		s = str(chute).strip().upper()
		if s in ('LOWER', 'BOTTOM'):
			return Chutes.LOWER
		if s in ('UPPER', 'TOP'):
			return Chutes.UPPER
		return Chutes(str(int(s)))

	@classmethod
	def _coerce_side(cls, side):
		if isinstance(side, Sides):
			return side
		s = str(side).strip().upper()
		return Sides(s)

	@classmethod
	def _coerce_dest(cls, dest):
		if isinstance(dest, Dests):
			return dest
		s = str(dest).strip()
		try:
			return Dests(s)
		except Exception:
			return s

	@classmethod
	def parse(cls, destination):
		if isinstance(destination, cls):
			return destination

		if isinstance(destination, dict) and 'destination' in destination:
			return cls.parse(destination['destination'])

		if not isinstance(destination, (str, unicode)):
			return cls.parse(str(destination))

		s = destination.strip()

		# Accept BOTH:
		#  - DST-0001-1-1-A
		#  - DST-0001-1-A  (defaults dest -> '1')
		parts = s.split('-')
		if len(parts) == 4 and parts[0].upper() == 'DST':
			_, station, chute, side = parts
			return cls(station, chute, Dests.REAR, side)

		match = SorterDataDestination_DefaultPattern.DESTINATION_PATTERN.match(s)
		if not match:
			raise KeyError('%r does not match the pattern expected; can not parse' % destination)

		mgd = match.groupdict()
		return cls(mgd['station'], mgd['chute'], mgd['dest'], mgd['side'])

	def __getitem__(self, key):
		assert key in self.LOOKUP_PROPERTIES, "Only station, chute, dest, and side are available for lookup"
		return getattr(self, key)

	def __iter__(self):
		for key in self.LOOKUP_PROPERTIES:
			yield key

	def __hash__(self):
		return hash(str(self))

	def __eq__(self, other):
		return str(self) == str(other)

	def __lt__(self, other):
		return str(self) < str(other)

	def __str__(self):
		return 'DST-%s-%s-%s-%s' % (self._station, self._chute, self._dest, self._side)

	def __repr__(self):
		return str(self)


class EuroSorterContentTracking(
	EuroSorterRoutingManagement,
	EuroSorterContextManagement,
	EuroSorterDestinationMapping,
):
	DESTINATION_CONTENT_CACHE_SCOPE = 'EuroSort-Contents'

	CARRIERS_CACHE_SCOPE = 'EuroSort-Carriers'
	CARRIERS_LIFESPAN_SEC = 60 * 60 * 24   # one day

	_SIDE_TOKENS = set([m for m in Sides])

	def __init__(self, name, **init_config):
		name = self._normalize_sorter_name(name)
		super(EuroSorterContentTracking, self).__init__(name, **init_config)
		self._initialize_destination_contents()
		self._initialize_carrier_contents()

	# ------------------------------------------------------------------
	# SORTER CONFIG
	# ------------------------------------------------------------------
	def _normalize_sorter_name(self, name):
		s = str(name).strip()
		for canonical_name, cfg in SORTER_CONFIG.items():
			for alias in cfg.get('aliases', ()):
				if s == alias:
					return canonical_name
		return s

	def _get_sorter_config(self):
		cfg = SORTER_CONFIG.get(self.name)
		if not cfg:
			raise ValueError('Sorter %s is not configured in SORTER_CONFIG' % self.name)
		return cfg

	def _get_sorter_mode(self):
		return self._get_sorter_config().get('mode')

	def _clone(self, value):
		try:
			return copy.deepcopy(value)
		except Exception:
			try:
				return json.loads(json.dumps(value, default=repr))
			except Exception:
				return value

	def _get_position_from_destination(self, dest_string):
		try:
			d = Destination.parse(dest_string)
			if str(d.dest) == '1':
				return 'REAR'
			elif str(d.dest) == '2':
				return 'FRONT'
		except Exception:
			pass
		return None

	def _flatten_destination_record_for_tags(self, record):
		flat = {}
		if not isinstance(record, dict):
			return flat

		for k, v in record.items():
			if k == 'chute_info':
				continue
			flat[k] = v

		chute_info = record.get('chute_info') or {}
		if isinstance(chute_info, dict):
			for k, v in chute_info.items():
				flat[k] = v

		return flat

	# ------------------------------------------------------------------
	# SHARED DESTINATION HELPERS
	# Centralized here so Level2, Level3, and Level3_Ship all inherit
	# the same implementation rather than each defining their own copy.
	# ------------------------------------------------------------------

	def _dest_info(self, rec):
		"""Returns the chute_info dict from a destination record, or {}."""
		if not isinstance(rec, dict):
			return {}
		info = rec.get('chute_info')
		return info if isinstance(info, dict) else {}

	def _dest_get(self, rec, key, default=None):
		"""
		Looks up a key in a destination record, checking the top-level
		fields first and falling back to chute_info. This means callers
		don't need to know whether a field lives at the common level or
		the sorter-specific level.
		"""
		if not isinstance(rec, dict):
			return default
		if key in rec:
			return rec.get(key, default)
		info = rec.get('chute_info')
		if isinstance(info, dict):
			return info.get(key, default)
		return default

	def _dest_update(self, destination, common_updates=None, chute_updates=None):
		"""
		Convenience wrapper around destination_update that keeps common-level
		and chute_info-level updates clearly separated. Merges chute_updates
		into the existing chute_info rather than replacing it wholesale.
		"""
		common_updates = common_updates or {}
		chute_updates  = chute_updates  or {}

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

	def _apply_physical_behavior_defaults(self, new_record):
		"""
		Sets has_upper_lower, has_front_rear, and has_gate from chute_type.
		Called automatically by destination_update() and _init_destination().
		Never call this manually — it runs on every update.
		"""
		chute_type = str(new_record.get('chute_type', 'NORMAL')).strip().upper()

		if chute_type == 'OB':
			# UC2.1 — single level deep, no batch door, no front/rear.
			# Holds multiple orders (UC3.1) in one undivided space.
			new_record['has_upper_lower'] = True
			new_record['has_front_rear']  = False
			new_record['has_gate']        = False

		elif chute_type == 'BAGGING':
			# Bagging re-induction lane — single pass, no front/rear, no gate
			new_record['has_upper_lower'] = False
			new_record['has_front_rear'] = False
			new_record['has_gate'] = False

		elif chute_type in ('JACKPOT', 'NOREAD', 'PURGE'):
			# Exception/purge lanes — items fall straight in, no batch door
			new_record['has_upper_lower'] = True
			new_record['has_front_rear'] = False
			new_record['has_gate'] = False

		elif chute_type == 'INSPECTION':
			# Inspection chutes have upper/lower but no batch door (UC9.10)
			new_record['has_upper_lower'] = True
			new_record['has_front_rear'] = False
			new_record['has_gate'] = False

		elif chute_type == 'HP':
			# High priority chutes have the same physical layout as NORMAL (UC9.4)
			new_record['has_upper_lower'] = True
			new_record['has_front_rear'] = True
			new_record['has_gate'] = True

		else:
			# NORMAL / PACKOUT — full front/rear with batch door (UC7.1, UC9.7)
			new_record['has_upper_lower'] = True
			new_record['has_front_rear'] = True
			new_record['has_gate'] = True

		return new_record

	# ------------------------------------------------------------------
	# CONFIG LOAD
	# ------------------------------------------------------------------
	def _load_routing_config(self):
		super(EuroSorterContentTracking, self)._load_routing_config()
		if self._read_config_tag('Reset/Clear and reload on next restart'):
			self.clear()
			self._write_config_tag('Reset/Clear and reload on next restart', False)
		else:
			self._initialize_destination_contents()
			self._initialize_carrier_contents()

	# ------------------------------------------------------------------
	# CORE DUMPS
	# ------------------------------------------------------------------
	@property
	def _core_dump_dir(self):
		core_dump_dir = self.config['log_path'] + '/' + 'coredump'
		if not os.path.exists(core_dump_dir):
			os.makedirs(core_dump_dir)
		return core_dump_dir

	def _dump_core(self):
		json_payload = self._generate_contents_json()
		timestamp = datetime.now().isoformat('_').replace(':', '')[:17]
		filepath = self._core_dump_dir + '/' + 'core_dump.' + timestamp + '.json'
		with open(filepath, 'w') as f:
			f.write(json_payload)
		self.logger.warn('Sorter data dumped core at {filepath}', filepath=filepath)

	def _generate_contents_json(self):
		info = {
			'_id': self.name,
			'chutes': dict(self._destination_contents),
			'carriers': dict(self._carrier_contents),
			'last_updated': system.date.now(),
		}
		return self._serialize_to_json(info)

	def _serialize_to_json(self, something):
		return json.dumps(something, indent=2, sort_keys=True, default=repr)

	def _on_jvm_shutdown(self):
		self._dump_core()
		super(EuroSorterContentTracking, self)._on_jvm_shutdown()

	# ------------------------------------------------------------------
	# MONGO HELPERS
	# ------------------------------------------------------------------
	def _load_sorter_doc(self):
		raw = select_record(MONGODB, MONGO_COLL, {'_id': self.name})
		doc_from_db = None

		if isinstance(raw, dict):
			doc_from_db = raw
		elif isinstance(raw, (list, tuple)):
			for item in raw:
				if isinstance(item, dict):
					doc_from_db = item
					break

		if doc_from_db:
			chutes   = doc_from_db.get('chutes')   or {}
			carriers = doc_from_db.get('carriers') or {}

			try:
				chutes = dict(chutes)
			except Exception:
				chutes = {}

			try:
				carriers = dict(carriers)
			except Exception:
				carriers = {}

			doc = {
				'_id':          self.name,
				'chutes':       chutes,
				'carriers':     carriers,
				'last_updated': system.date.now(),
			}
			return True, doc

		doc = {
			'_id':          self.name,
			'chutes':       {},
			'carriers':     {},
			'last_updated': system.date.now(),
		}
		return False, doc

	def _serialize_destination_for_mongo(self, record):
		if record is None:
			return {}
		try:
			return dict(record)
		except Exception:
			return {'value': repr(record)}

	def _serialize_carrier_for_mongo(self, record):
		if record is None:
			return {}
		try:
			return dict(record)
		except Exception:
			return {'value': repr(record)}

	def _sync_destination_to_mongo(self, dest_key):
		dest_key = str(dest_key)
		dest_rec = self._destination_contents.get(dest_key)
		if dest_rec is None:
			return

		status, doc = self._load_sorter_doc()
		chutes = doc.get('chutes', {}) if status else {}

		chutes[dest_key] = self._serialize_destination_for_mongo(dest_rec)

		doc['chutes'] = chutes
		doc['last_updated'] = system.date.now()

		upsert_record(MONGODB, MONGO_COLL, doc, {'_id': self.name})

	def _sync_carrier_to_mongo(self, carrier_number):
		num = self._coerce_carrier_number(carrier_number)
		carrier_rec = self._carrier_contents.get(num)
		if carrier_rec is None:
			return

		status, doc = self._load_sorter_doc()
		carriers = doc.get('carriers', {}) if status else {}

		carriers[str(num)] = self._serialize_carrier_for_mongo(carrier_rec)

		doc['carriers'] = carriers
		doc['last_updated'] = system.date.now()

		upsert_record(MONGODB, MONGO_COLL, doc, {'_id': self.name})

	def _sync_all_to_mongo(self):
		chutes_doc = {}
		for k, rec in self._destination_contents.items():
			chutes_doc[k] = self._serialize_destination_for_mongo(rec)

		carriers_doc = {}
		for num, rec in self._carrier_contents.items():
			carriers_doc[str(num)] = self._serialize_carrier_for_mongo(rec)

		doc = {
			'_id': self.name,
			'chutes': chutes_doc,
			'carriers': carriers_doc,
			'last_updated': system.date.now(),
		}
		upsert_record(MONGODB, MONGO_COLL, doc, {'_id': self.name})

	# ------------------------------------------------------------------
	# DESTINATION CONTENTS (ExtraGlobal)
	# ------------------------------------------------------------------
	@property
	def _destination_contents(self):
		try:
			return ExtraGlobal.access(self.name, self.DESTINATION_CONTENT_CACHE_SCOPE)
		except KeyError:
			self.logger.warn('Destination contents not initialized. Setting up...')
			self._initialize_destination_contents(full_clear=True)
			return ExtraGlobal.access(self.name, self.DESTINATION_CONTENT_CACHE_SCOPE)

	def clear(self):
		self.logger.warn('Clearing all tracking data from sorter %s' % self.name)
		self.log_event('tracking', reason='clear')
		self._dump_core()

		try:
			ExtraGlobal.trash(self.name, self.DESTINATION_CONTENT_CACHE_SCOPE)
		except KeyError:
			pass
		try:
			ExtraGlobal.trash(self.name, self.CARRIERS_CACHE_SCOPE)
		except KeyError:
			pass

		self._initialize_destination_contents(full_clear=True)
		self._initialize_carrier_contents(full_clear=True)

		self._sync_all_to_mongo()

	def _get_wcs_name(self, dest_string):
		dest_string = str(dest_string)
		cfg = self._get_sorter_config()
		machine_name = cfg.get('wcs_prefix', 'X')

		parts = dest_string.split('-')
		station = int(parts[1])
		chute = int(parts[2])
		dest = int(parts[3]) if len(parts) > 3 else 1
		side = parts[4] if len(parts) > 4 else 'A'

		chutename = "{machine_name}{station:04d}{chute}{dest}{side}".format(
			machine_name=machine_name,
			station=station,
			chute=chute,
			dest=dest,
			side=side
		)
		return chutename

	def _init_destination(self, dest_string):
		cfg = self._get_sorter_config()
		sorter_default = self._clone(cfg.get('chute_default') or {})
		wcs = self._get_wcs_name(str(dest_string))

		base = self._clone(COMMON_CHUTE_DEFAULT)
		base['_id'] = str(dest_string)
		base['destination'] = str(dest_string)
		base['chuteName'] = wcs
		base['sorter'] = self.name
		base['position'] = self._get_position_from_destination(dest_string)
		base['chute_info'] = sorter_default
		base['last_updated'] = None

		base = self._apply_physical_behavior_defaults(base)

		return base

	def _normalize_loaded_destination_record(self, dest_key, rec_dict):
		base = self._init_destination(dest_key)

		if not isinstance(rec_dict, dict):
			return base

		base.update({
			'_id': rec_dict.get('_id', base['_id']),
			'destination': rec_dict.get('destination', base['destination']),
			'chuteName': rec_dict.get('chuteName', base['chuteName']),
			'sorter': rec_dict.get('sorter', base['sorter']),
			'faulted': rec_dict.get('faulted', base['faulted']),
			'in_service': rec_dict.get('in_service', rec_dict.get('enabled', base['in_service'])),
			'position': rec_dict.get('position', base['position']),
			'chute_type': rec_dict.get('chute_type', base['chute_type']),
			'lane': rec_dict.get('lane', base['lane']),
			'occupied': rec_dict.get('occupied', base['occupied']),
			'available': rec_dict.get('available', base['available']),
			'dfs': rec_dict.get('dfs', base['dfs']),
			'ofs': rec_dict.get('ofs', base['ofs']),
			'first_item_delivered_ts': rec_dict.get('first_item_delivered_ts', base['first_item_delivered_ts']),
			'enroute': rec_dict.get('enroute', base['enroute']),
			'delivered': rec_dict.get('delivered', base['delivered']),
			'last_updated': rec_dict.get('last_updated', base['last_updated']),
		})

		chute_info = base.get('chute_info') or {}
		loaded_chute_info = rec_dict.get('chute_info')

		if isinstance(loaded_chute_info, dict):
			chute_info.update(loaded_chute_info)

		for k, v in rec_dict.items():
			if k in (
				'_id', 'destination', 'chuteName', 'sorter',
				'faulted', 'in_service', 'enabled', 'position', 'chute_type',
				'lane', 'occupied', 'available', 'dfs', 'ofs',
				'first_item_delivered_ts',
				'enroute', 'delivered', 'last_updated'
			):
				continue
			if k == 'chute_info':
				continue
			chute_info[k] = v

		base['chute_info'] = chute_info
		base = self._apply_physical_behavior_defaults(base)
		return base

	def _initialize_destination_contents(self, full_clear=False):
		if not full_clear:
			try:
				destination_contents = ExtraGlobal.access(self.name, self.DESTINATION_CONTENT_CACHE_SCOPE)
			except KeyError:
				full_clear = True

		if full_clear:
			self.logger.warn('Reinitializing destination contents for sorter %s' % self.name)
			self.log_event('tracking', reason='reinitialize-contents')

			destination_contents = {}
			ExtraGlobal.stash(
				destination_contents,
				self.name, self.DESTINATION_CONTENT_CACHE_SCOPE,
				lifespan=self.CARRIERS_LIFESPAN_SEC,
			)

		for dest_string in self._destination_mapping:
			if dest_string not in destination_contents:
				destination_contents[dest_string] = self._init_destination(dest_string)

		try:
			status, doc = self._load_sorter_doc()
			mongo_chutes = doc.get('chutes')
			if mongo_chutes:
				for dest_key, rec_dict in mongo_chutes.items():
					destination_contents[dest_key] = self._normalize_loaded_destination_record(dest_key, rec_dict)
		except Exception as e:
			self.logger.warn(
				'Failed to hydrate destination contents from Mongo for sorter {name}: {err}',
				name=self.name,
				err=e
			)

		self.logger.trace(
			'Initialized/verified destination metadata for {n} destinations (with Mongo hydration)',
			n=len(destination_contents)
		)

	def destination_get(self, identifier):
		if isinstance(identifier, Destination):
			key = identifier.destination
		elif isinstance(identifier, dict) and 'destination' in identifier:
			key = identifier['destination']
		else:
			key = identifier
		return self._destination_contents.get(key)

	def destination_update(self, identifier, updates=None, **extra_updates):
		if isinstance(identifier, Destination):
			dest_key = identifier.destination
		elif isinstance(identifier, dict) and 'destination' in identifier:
			dest_key = identifier['destination']
		else:
			dest_key = identifier

		dest_contents = self._destination_contents

		record = dest_contents.get(dest_key)
		if record is None:
			record = self._init_destination(dest_key)

		if not isinstance(record, dict):
			try:
				record = dict(record)
			except Exception:
				record = self._init_destination(dest_key)

		merged = {}
		if isinstance(updates, dict):
			merged.update(updates)
		merged.update(extra_updates)

		common_keys = set(COMMON_CHUTE_DEFAULT.keys())
		new_record = self._clone(record)
		chute_info = new_record.get('chute_info') or {}
		if not isinstance(chute_info, dict):
			chute_info = {}

		for key, value in merged.items():
			if key == 'chute_info' and isinstance(value, dict):
				chute_info.update(value)
			elif key in common_keys:
				new_record[key] = value
			else:
				chute_info[key] = value

		mode = self._get_sorter_mode()

		if mode == 'level2':
			if not bool(chute_info.get('assigned')):
				chute_info['assigned_name'] = []

		elif mode == 'level3':
			if new_record.get('occupied') is False:
				chute_info['zone'] = ''
				chute_info['group_id'] = ''

		elif mode == 'level3_ship':
			pass

		new_record['chute_info'] = chute_info
		new_record['_id'] = str(dest_key)
		new_record['destination'] = str(dest_key)
		new_record['chuteName'] = new_record.get('chuteName') or self._get_wcs_name(str(dest_key))
		new_record['sorter'] = self.name
		new_record['position'] = new_record.get('position') or self._get_position_from_destination(dest_key)
		new_record['last_updated'] = system.date.now()

		new_record = self._apply_physical_behavior_defaults(new_record)

		if mode in ('level2', 'level3', 'level3_ship'):
			chute_info['lastUpdated'] = datetime.now()

		dest_contents[dest_key] = new_record

		self._sync_destination_to_mongo(dest_key)

		try:
			base_tag_path = '[EuroSort]EuroSort/%s/Destinations/%s/' % (self.name, dest_key)
			tag_field_map = self._get_sorter_config().get('tag_field_map') or {}
			flat_record = self._flatten_destination_record_for_tags(new_record)

			write_paths = []
			write_values = []

			for field_name, tag_suffix in tag_field_map.items():
				if field_name not in flat_record:
					continue

				value = flat_record.get(field_name)

				if isinstance(value, bool):
					value = bool(value)
				elif isinstance(value, (int, long, float)):
					value = value
				elif isinstance(value, (list, dict, tuple)):
					value = json.dumps(value, default=repr)
				elif value is None:
					value = ''
				else:
					value = str(value)

				write_paths.append(base_tag_path + tag_suffix)
				write_values.append(value)

			if write_paths:
				system.tag.writeBlocking(write_paths, write_values)

		except Exception:
			pass

		return new_record

	def clear_level2_assignment(self, dest_key):
		if self._get_sorter_mode() != 'level2':
			return None
		return self.destination_update(
			dest_key,
			assigned=False,
			assigned_name=[],
			assigned_mode='',
		)

	def clear_level3_occupancy(self, dest_key):
		if self._get_sorter_mode() != 'level3':
			return None
		return self.destination_update(
			dest_key,
			occupied=False,
			available=True,
			zone='',
			group_id='',
			ibns='',
			chuteCount=0,
			volume=0.0,
			volume_percent_full=0.0,
			chuteFull=False,
			toteFull=False,
		)

	def clear_level3_ship_occupancy(self, dest_key):
		if self._get_sorter_mode() != 'level3_ship':
			return None
		return self.destination_update(
			dest_key,
			occupied=False,
			available=True,
			first_item_delivered_ts=None,
			ready_for_packout=False,
			missing_ibns=[],
			expected_line_count=0,
			order_count_total=0,
			item_count_total=0,
			line_count_total=0,
			percent_orders_consolidated=0.0,
			oldest_order_age_sec=0,
			batch_door_state='DOWN',
			sort_codes=[],
			contains_priority_order=False,
			orders=[],
			ibns=[],
		)

	# ------------------------------------------------------------------
	# LEVEL3_SHIP SORT CODE ENFORCEMENT (UC9.2)
	# ------------------------------------------------------------------
	def chute_has_sort_code(self, dest_key, sort_code):
		"""
		Returns True if the given sort code is already present in this chute.
		Used to enforce UC9.2 — a chute may only hold one order per sort code.
		"""
		if self._get_sorter_mode() != 'level3_ship':
			return False
		rec = self.destination_get(dest_key)
		if rec is None:
			return False
		chute_info = rec.get('chute_info') or {}
		sort_codes = chute_info.get('sort_codes') or []
		return sort_code in sort_codes

	def add_sort_code_to_chute(self, dest_key, sort_code):
		"""
		Adds a sort code to the chute's tracked list.
		Should be called when an order is assigned to this chute (UC9.2).
		"""
		if self._get_sorter_mode() != 'level3_ship':
			return None
		rec = self.destination_get(dest_key)
		if rec is None:
			return None
		chute_info = rec.get('chute_info') or {}
		sort_codes = list(chute_info.get('sort_codes') or [])
		if sort_code not in sort_codes:
			sort_codes.append(sort_code)
		return self.destination_update(dest_key, sort_codes=sort_codes)

	def remove_sort_code_from_chute(self, dest_key, sort_code):
		"""
		Removes a sort code from the chute's tracked list.
		Should be called when an order is fully consolidated and removed (UC9.2).
		"""
		if self._get_sorter_mode() != 'level3_ship':
			return None
		rec = self.destination_get(dest_key)
		if rec is None:
			return None
		chute_info = rec.get('chute_info') or {}
		sort_codes = list(chute_info.get('sort_codes') or [])
		if sort_code in sort_codes:
			sort_codes.remove(sort_code)
		return self.destination_update(dest_key, sort_codes=sort_codes)

	# ------------------------------------------------------------------
	# LEVEL3_SHIP PRIORITY ESCALATION (UC10.3, UC10.4)
	# ------------------------------------------------------------------
	def flag_chute_priority_escalation(self, dest_key):
		"""
		Flags a chute as containing a high-priority order per UC10.3.
		Called when an order's status changes to MST/MSQ while already
		in a consolidation chute. The UI layer is responsible for rendering
		the red overlay and flashing light based on this flag.
		"""
		if self._get_sorter_mode() != 'level3_ship':
			return None
		return self.destination_update(dest_key, contains_priority_order=True)

	def clear_chute_priority_escalation(self, dest_key):
		"""
		Clears the priority escalation flag once the high-priority order
		has been packed out or the chute is cleared.
		"""
		if self._get_sorter_mode() != 'level3_ship':
			return None
		return self.destination_update(dest_key, contains_priority_order=False)

	# ------------------------------------------------------------------
	# LEVEL3_SHIP CHUTE TYPE GUARD (UC8.1)
	# ------------------------------------------------------------------
	def _assert_operator_chute_type(self, chute_type):
		"""
		Raises ValueError if chute_type is not in OPERATOR_SELECTABLE_CHUTE_TYPES.
		Used by Andrew's set_chute_type() to prevent OB, NOREAD, and BAGGING
		from being set via the UI (UC8.1).
		"""
		ct = str(chute_type).strip().upper()
		if ct not in OPERATOR_SELECTABLE_CHUTE_TYPES:
			if ct in SYSTEM_ASSIGNED_CHUTE_TYPES:
				raise ValueError(
					'chute_type %r is system-assigned and cannot be set by an operator. '
					'Operator-selectable types: %s'
					% (chute_type, ', '.join(sorted(OPERATOR_SELECTABLE_CHUTE_TYPES)))
				)
			raise ValueError(
				'Unknown chute_type %r. Operator-selectable types: %s'
				% (chute_type, ', '.join(sorted(OPERATOR_SELECTABLE_CHUTE_TYPES)))
			)
		return ct

	# ------------------------------------------------------------------
	# CARRIER CONTENTS (ExtraGlobal)
	# ------------------------------------------------------------------
	@property
	def _carrier_contents(self):
		try:
			return ExtraGlobal.access(self.name, self.CARRIERS_CACHE_SCOPE)
		except KeyError:
			self.logger.warn('Carriers cache not initialized. Setting up...')
			self._initialize_carrier_contents(full_clear=True)
			return ExtraGlobal.access(self.name, self.CARRIERS_CACHE_SCOPE)

	def _initialize_carrier_contents(self, full_clear=True):
		if not full_clear:
			try:
				carrier_contents = ExtraGlobal.access(self.name, self.CARRIERS_CACHE_SCOPE)
			except KeyError:
				full_clear = True

		if full_clear:
			self.logger.warn('Reinitializing carrier data for sorter %s' % self.name)
			self.log_event('tracking', reason='reinitialize-carriers')

			carrier_contents = {}
			ExtraGlobal.stash(
				carrier_contents,
				self.name, self.CARRIERS_CACHE_SCOPE,
				lifespan=self.CARRIERS_LIFESPAN_SEC,
			)

		cfg = self._get_sorter_config()

		self.CARRIERS_MIN = 1
		self.CARRIERS_MAX = cfg.get('carrier_max')
		if not self.CARRIERS_MAX:
			raise ValueError('carrier_max not configured for sorter %s' % self.name)

		# Carriers are created on first use — no pre-population.
		# On restart, only rehydrate carriers that were actively carrying an item
		# (destination != None). Idle carriers are not worth restoring.
		try:
			status, doc = self._load_sorter_doc()
			mongo_carriers = doc.get('carriers')
			if mongo_carriers:
				skipped = 0
				for num_str, rec_dict in mongo_carriers.items():
					if not isinstance(rec_dict, dict):
						continue
					try:
						num = int(num_str)
					except Exception:
						continue
					if num < self.CARRIERS_MIN or num > self.CARRIERS_MAX:
						continue
					base = self._init_carrier(num)
					base.update(rec_dict)
					if not self._carrier_is_active(base):
						skipped += 1
						continue
					carrier_contents[num] = base
		except Exception as e:
			self.logger.warn(
				'Failed to hydrate carrier contents from Mongo for sorter {name}: {err}',
				name=self.name,
				err=e
			)

		self.logger.trace(
			'Carrier store ready for sorter {name} — {n} active carriers restored from Mongo, {s} idle skipped (lazy init active)',
			name=self.name,
			n=len(carrier_contents),
			s=skipped,
		)

	def _init_carrier(self, n):
		return {
			'carrier_number': n,
			'issue_info': {},
			'track_id': None,
			'in_service': True,
			'assigned_name': None,
			'assigned_mode': None,
			'recirculation_count': 0,
			'destination': None,
			'induct_scanner': None,
			'delivered': 0,
			'discharged_attempted': False,
			'failed_deliveries': 0,
			'deliveries_aborted': 0,
			'ob_reinducted': False,
			'last_updated': None,
		}

	def _carrier_is_active(self, rec):
		"""
		Returns True if this carrier is currently carrying an item — i.e. it has
		a destination assigned. Idle carriers (destination=None) are not kept in
		cache and are not persisted to Mongo.
		"""
		if not isinstance(rec, dict):
			return False
		return rec.get('destination') is not None

	def _evict_carrier(self, num):
		"""
		Removes a carrier from the in-memory cache once it is no longer active.
		The Mongo record is intentionally kept — it holds lifetime metrics
		(delivered, failed_deliveries, recirculation_count, etc.) that must
		survive restarts. The carrier will be re-created in cache on next use.
		"""
		carriers = self._carrier_contents
		if num in carriers:
			del carriers[num]

	def carriers_clear(self):
		"""Drops all carrier records from cache and Mongo. Carriers will be re-created on next use."""
		self.logger.warn('Clearing carriers cache for sorter {name}', name=self.name)
		try:
			ExtraGlobal.trash(self.name, self.CARRIERS_CACHE_SCOPE)
		except KeyError:
			pass
		self._initialize_carrier_contents(full_clear=True)
		self._sync_all_to_mongo()

	def carriers_all(self):
		"""Returns only carriers that have been created (i.e. seen at least once). Not all carrier_max."""
		return self._carrier_contents

	def carrier_usage_percent(self):
		"""
		Returns the percentage of the sorter's carrier capacity that is currently
		active (i.e. has a destination assigned), as a float 0.0–100.0.

		Based on CARRIERS_MAX, not the number of carriers seen — this gives a
		true utilisation figure against the physical tray count.

		Returns:
			float — e.g. 42.7 means 42.7% of trays are carrying an item
		"""
		active = sum(
			1 for rec in self._carrier_contents.values()
			if self._carrier_is_active(rec)
		)
		if not self.CARRIERS_MAX:
			return 0.0
		return round((active / float(self.CARRIERS_MAX)) * 100.0, 2)

	def purge_active_carriers(self, jackpot_dest_key):
		"""
		Emergency purge of all carriers that are currently active but have not
		been delivered. For each such carrier:
		  1. Reads the track_id so the WCS can divert the physical tray.
		  2. Updates the carrier's destination to the given jackpot_dest_key.
		  3. Writes the change through to Mongo via carrier_update.

		The caller is responsible for sending the actual WCS divert command using
		the returned track_ids. This method only updates the logical state.

		Args:
			jackpot_dest_key: dest_key string of the target JACKPOT chute
			                  (must have chute_type == 'JACKPOT' or 'NOREAD')

		Returns:
			list of dicts: [{'carrier_number': int, 'track_id': str|None,
			                 'previous_destination': str|None}, ...]
			One entry per carrier that was diverted.
		"""
		diverted = []

		for num, rec in list(self._carrier_contents.items()):
			if not self._carrier_is_active(rec):
				continue

			track_id = rec.get('track_id')
			previous_dest = rec.get('destination')

			self.carrier_update(num, destination=jackpot_dest_key)

			diverted.append({
				'carrier_number': num,
				'track_id':            track_id,
				'previous_destination': previous_dest,
			})

		self.logger.warn(
			'purge_active_carriers: diverted {n} carriers to {dest} for sorter {name}',
			n=len(diverted), dest=jackpot_dest_key, name=self.name,
		)
		self.log_event('tracking', reason='purge-active-carriers', count=len(diverted))

		return diverted

	def reset_carrier_metrics(self, carrier_number):
		"""
		Resets all lifetime metric counters on a carrier record back to zero
		without removing the record from Mongo. Use this for per-carrier
		maintenance (e.g. end-of-day or after a carrier swap).

		Resets: delivered, failed_deliveries, deliveries_aborted, recirculation_count.
		Does NOT reset: ob_reinducted, destination, track_id, or any active state.

		Args:
			carrier_number: int or numeric string

		Returns:
			updated carrier record
		"""
		num = self._coerce_carrier_number(carrier_number)
		return self.carrier_update(
			num,
			delivered=0,
			failed_deliveries=0,
			deliveries_aborted=0,
			recirculation_count=0,
		)

	def reset_all_carrier_metrics(self):
		"""
		Resets lifetime metric counters for every carrier that has a Mongo record.
		Loads all carriers from Mongo (including idle ones not in cache), resets
		their metrics, and writes them back. Active carriers in cache are also
		updated immediately.

		Use at end-of-day or after a physical tray swap-out.

		Returns:
			int — number of carrier records reset
		"""
		count = 0

		# Reset any active carriers currently in cache
		for num in list(self._carrier_contents.keys()):
			self.carrier_update(
				num,
				delivered=0,
				failed_deliveries=0,
				deliveries_aborted=0,
				recirculation_count=0,
			)
			count += 1

		# Also reset idle carriers sitting in Mongo (not in cache)
		try:
			status, doc = self._load_sorter_doc()
			if status:
				mongo_carriers = doc.get('carriers') or {}
				changed = False
				for num_str, rec_dict in mongo_carriers.items():
					if not isinstance(rec_dict, dict):
						continue
					try:
						num = int(num_str)
					except Exception:
						continue
					# Skip if already handled via cache above
					if num in self._carrier_contents:
						continue
					rec_dict['delivered'] = 0
					rec_dict['failed_deliveries'] = 0
					rec_dict['deliveries_aborted'] = 0
					rec_dict['recirculation_count'] = 0
					rec_dict['last_updated'] = system.date.now()
					mongo_carriers[num_str] = rec_dict
					count += 1
					changed = True

				if changed:
					doc['carriers'] = mongo_carriers
					doc['last_updated'] = system.date.now()
					upsert_record(MONGODB, MONGO_COLL, doc, {'_id': self.name})
		except Exception as e:
			self.logger.warn(
				'reset_all_carrier_metrics: failed to reset Mongo-only carriers for {name}: {err}',
				name=self.name, err=e,
			)

		self.logger.warn(
			'reset_all_carrier_metrics: reset {n} carrier records for sorter {name}',
			n=count, name=self.name,
		)
		self.log_event('tracking', reason='reset-carrier-metrics', count=count)
		return count

	def carrier_get(self, carrier_number):
		"""
		Returns the carrier record for carrier_number, or None if this carrier
		has never been inducted. Callers must handle None — do not assume a
		carrier record exists just because the number is in range.

		Records are created on first write via carrier_update(), not on read.
		"""
		if not carrier_number:
			return None
		num = self._coerce_carrier_number(carrier_number)
		return self._carrier_contents.get(num)

	def _coerce_carrier_number(self, value):
		if isinstance(value, (int, long)):
			num = value
		elif isinstance(value, (str, unicode)):
			s = value.strip()
			if not s.isdigit():
				raise ValueError('Carrier number must be numeric string: %r' % value)
			num = int(s)
		else:
			raise TypeError('Carrier number must be int or numeric string, not %r' % type(value))

		if not (self.CARRIERS_MIN <= num <= self.CARRIERS_MAX):
			raise ValueError(
				'Carrier number out of range (%d..%d): %r'
				% (self.CARRIERS_MIN, self.CARRIERS_MAX, num)
			)
		return num

	def carrier_update(self, carrier_number, updates=None, **extra_updates):
		num = self._coerce_carrier_number(carrier_number)
		carriers = self._carrier_contents

		record = carriers.get(num)
		if record is None:
			# First time this carrier has been seen — create it on demand.
			self.logger.trace('Creating carrier record on first use: {num}', num=num)
			record = self._init_carrier(num)

		if not isinstance(record, dict):
			try:
				record = dict(record)
			except Exception:
				record = self._init_carrier(num)

		merged = {}
		if isinstance(updates, dict):
			merged.update(updates)
		merged.update(extra_updates)
		merged['last_updated'] = system.date.now()

		record.update(merged)
		carriers[num] = record

		self._sync_carrier_to_mongo(num)

		return record

	def update_carrier_and_destination(self,
	                                   carrier_number,
	                                   dest_identifier=None,
	                                   carrier_updates=None,
	                                   dest_updates=None):
		rec_carrier = None
		rec_dest = None

		if carrier_updates:
			rec_carrier = self.carrier_update(carrier_number, carrier_updates)

		if dest_identifier is not None and dest_updates:
			rec_dest = self.destination_update(dest_identifier, dest_updates)

		return rec_carrier, rec_dest

	def assign_carrier_to_destination(self,
	                                  carrier_number,
	                                  dest_identifier,
	                                  scanner=None,
	                                  track_id=None,
	                                  assigned_name=None,
	                                  assigned_mode=None,
	                                  transit_info=None,
	                                  extra_carrier_updates=None,
	                                  extra_dest_updates=None):
		if extra_carrier_updates is None:
			extra_carrier_updates = {}
		if extra_dest_updates is None:
			extra_dest_updates = {}
		if transit_info is None:
			transit_info = {}

		carrier_updates = dict(extra_carrier_updates)
		carrier_updates['destination'] = dest_identifier
		carrier_updates['issue_info'] = transit_info
		carrier_updates['assigned_name'] = assigned_name
		carrier_updates['assigned_mode'] = assigned_mode

		if track_id is not None:
			carrier_updates['track_id'] = track_id

		if scanner:
			rec_carrier = self.carrier_get(carrier_number)
			existing_scanner = rec_carrier.get('induct_scanner') if rec_carrier else None
			if existing_scanner in (None, '', 'null'):
				carrier_updates['induct_scanner'] = scanner

		dest_rec = self.destination_get(dest_identifier) or {}

		dest_updates = dict(extra_dest_updates)

		if transit_info:
			existing_ci = dest_rec.get('chute_info') or {}
			if not isinstance(existing_ci, dict):
				existing_ci = {}

			caller_ci = dest_updates.get('chute_info') or {}
			if not isinstance(caller_ci, dict):
				caller_ci = {}

			merged_ci = {}
			merged_ci.update(existing_ci)
			merged_ci.update(caller_ci)

			current_transit = merged_ci.get('transit_info', {}) or {}
			if not isinstance(current_transit, dict):
				try:
					current_transit = dict(current_transit)
				except Exception:
					current_transit = {}

			current_transit.update(transit_info)
			merged_ci['transit_info'] = current_transit
			dest_updates['chute_info'] = merged_ci

		return self.update_carrier_and_destination(
			carrier_number,
			dest_identifier,
			carrier_updates=carrier_updates,
			dest_updates=dest_updates
		)

	def mark_carrier_ob_reinducted(self, carrier_number):
		"""
		FIX #3: Marks a carrier as having been re-inducted from an OB chute.
		Per UC5.6, once set this carrier must never be diverted to OB again —
		it recirculates indefinitely until a consolidation destination is available.
		The routing layer should check carrier.get('ob_reinducted') before
		sending an item to OB.
		"""
		num = self._coerce_carrier_number(carrier_number)
		return self.carrier_update(num, ob_reinducted=True)


	def mark_carrier_attempted(self, carrier_number, **extra_carrier_updates):
		num = self._coerce_carrier_number(carrier_number)
		rec = self.carrier_get(num)
		if rec is None:
			rec = self._init_carrier(num)

		updates = dict(extra_carrier_updates or {})
		updates['discharged_attempted'] = True

		return self.carrier_update(num, updates)

	def mark_carrier_delivered(self, carrier_number, **extra_carrier_updates):
		num = self._coerce_carrier_number(carrier_number)
		rec_carrier = self.carrier_get(num)
		if rec_carrier is None:
			rec_carrier = self._init_carrier(num)

		dest_identifier = rec_carrier.get('destination')

		current_delivered = rec_carrier.get('delivered', 0) or 0
		carrier_updates = dict(extra_carrier_updates or {})
		carrier_updates['delivered'] = current_delivered + 1
		carrier_updates['discharged_attempted'] = False
		carrier_updates['assigned_name'] = None
		carrier_updates['assigned_mode'] = None
		# Clear destination so the carrier is no longer considered active.
		# This is what gates both cache eviction and Mongo hydration on restart.
		carrier_updates['destination'] = None

		dest_updates = None
		if dest_identifier:
			dest_rec = self.destination_get(dest_identifier)
			if dest_rec is not None:
				dest_delivered = dest_rec.get('delivered', 0) or 0
				dest_updates = {
					'delivered': dest_delivered + 1,
				}

				if not dest_rec.get('first_item_delivered_ts'):
					dest_updates['first_item_delivered_ts'] = system.date.now()

		# Write the final metrics to carrier record and destination, then flush to Mongo.
		self.update_carrier_and_destination(
			carrier_number=num,
			dest_identifier=dest_identifier,
			carrier_updates=carrier_updates,
			dest_updates=dest_updates,
		)

		# Evict from cache — the carrier is now idle. Mongo retains the record
		# for lifetime metrics (delivered count, recirculation_count, etc.).
		self._evict_carrier(num)

	def mark_carrier_failed(self, carrier_number, **extra_carrier_updates):
		num = self._coerce_carrier_number(carrier_number)
		rec_carrier = self.carrier_get(num)
		if rec_carrier is None:
			rec_carrier = self._init_carrier(num)

		dest_identifier = rec_carrier.get('destination')

		current_failed = rec_carrier.get('failed_deliveries', 0) or 0
		carrier_updates = dict(extra_carrier_updates or {})
		carrier_updates['failed_deliveries'] = current_failed + 1
		carrier_updates['discharged_attempted'] = False

		dest_updates = None
		if dest_identifier:
			dest_rec = self.destination_get(dest_identifier)
			if dest_rec is not None:
				dest_updates = {}

		return self.update_carrier_and_destination(
			carrier_number=num,
			dest_identifier=dest_identifier,
			carrier_updates=carrier_updates,
			dest_updates=dest_updates,
		)

	def mark_carrier_aborted(self, carrier_number, **extra_carrier_updates):
		num = self._coerce_carrier_number(carrier_number)
		rec_carrier = self.carrier_get(num)
		if rec_carrier is None:
			rec_carrier = self._init_carrier(num)

		dest_identifier = rec_carrier.get('destination')

		current_aborted = rec_carrier.get('deliveries_aborted', 0) or 0
		carrier_updates = dict(extra_carrier_updates or {})
		carrier_updates['deliveries_aborted'] = current_aborted + 1
		carrier_updates['discharged_attempted'] = False

		dest_updates = None
		if dest_identifier:
			dest_rec = self.destination_get(dest_identifier)
			if dest_rec is not None:
				dest_updates = {}

		return self.update_carrier_and_destination(
			carrier_number=num,
			dest_identifier=dest_identifier,
			carrier_updates=carrier_updates,
			dest_updates=dest_updates,
		)

	def mark_carrier_unknown(self, carrier_number, **extra_carrier_updates):
		num = self._coerce_carrier_number(carrier_number)
		rec_carrier = self.carrier_get(num)
		if rec_carrier is None:
			rec_carrier = self._init_carrier(num)

		dest_identifier = rec_carrier.get('destination')

		issue_info = rec_carrier.get('issue_info', {}) or {}
		if not isinstance(issue_info, dict):
			try:
				issue_info = dict(issue_info)
			except Exception:
				issue_info = {}

		issue_info.setdefault('status', 'UNKNOWN')

		carrier_updates = dict(extra_carrier_updates or {})
		carrier_updates.setdefault('issue_info', issue_info)
		carrier_updates['discharged_attempted'] = False

		dest_updates = None
		if dest_identifier:
			dest_rec = self.destination_get(dest_identifier)
			if dest_rec is not None:
				dest_updates = {}

		return self.update_carrier_and_destination(
			carrier_number=num,
			dest_identifier=dest_identifier,
			carrier_updates=carrier_updates,
			dest_updates=dest_updates,
		)

	# ------------------------------------------------------------------
	# SUMMARY / INTROSPECTION
	# ------------------------------------------------------------------
	def destinations_all_transit_info(self):
		out = {}
		for dest_key, rec in self._destination_contents.items():
			if rec is None:
				out[dest_key] = {}
				continue

			chute_info = rec.get('chute_info', {}) or {}
			ti = chute_info.get('transit_info', {}) or {}

			try:
				out[dest_key] = dict(ti)
			except Exception:
				out[dest_key] = {}

		return out

	def destinations_all_chute_info(self):
		out = {}
		for dest_key, rec in self._destination_contents.items():
			if rec is None:
				out[dest_key] = {}
				continue

			ci = rec.get('chute_info', {}) or {}
			try:
				out[dest_key] = dict(ci)
			except Exception:
				out[dest_key] = {}

		return out

	def _sorted_destinations(self):
		def sort_key(dest_key):
			try:
				d = Destination.parse(dest_key)
				return (int(d.station), int(d.chute.value), int(d.dest), d.side.value)
			except Exception:
				return (9999, 9, 9999, dest_key)

		return sorted(self.destinations_all_transit_info().keys(), key=sort_key)

	def _apply_priority_escalation(self, order_number, priority, allowed_chute_types=None):
	priority = str(priority or '').strip().upper()
	if priority not in ('1', '2', '3', '4', '5'):
		return []

	order_number = str(order_number or '').strip()
	if not order_number:
		return []

	if allowed_chute_types is None:
		allowed_chute_types = ('NORMAL', 'OB')

	escalated = []

	for dest_key, rec in list(self._destination_contents.items()):
		if rec is None:
			continue

		chute_type = str(rec.get('chute_type', '')).upper()
		if chute_type not in allowed_chute_types:
			continue

		chute_info = self._dest_info(rec)

		if bool(chute_info.get('contains_priority_order', False)):
			continue

		orders_in_chute = [
			str(o.get('order_number') or '').strip()
			for o in (chute_info.get('orders') or [])
			if isinstance(o, dict)
		]

		if order_number not in orders_in_chute:
			continue

		try:
			self.flag_chute_priority_escalation(dest_key)
			self._set_chute_light_mode(dest_key, 'BLINK1')
			escalated.append(dest_key)

		except Exception as e:
			self.logger.warn(
				'_apply_priority_escalation: failed for chute=%s order=%s: %s'
				% (dest_key, order_number, str(e))
			)

	return escalated


	def handle_priority_escalation(self, order_number, priority):
		"""
		Scanner/data-point triggered priority escalation.
		Checks both NORMAL and OB chutes.
		"""
		return self._apply_priority_escalation(
			order_number,
			priority,
			allowed_chute_types=('NORMAL', 'OB'),
		)


	def _check_order_aging(self):
		"""
		Periodic priority scan.
		Scans active NORMAL chute contents only.
		"""
		for dest_key, rec in list(self._destination_contents.items()):
			if rec is None:
				continue
	
			if not self._dest_is_eligible(rec):
				continue
	
			chute_type = str(rec.get('chute_type', '')).upper()
			if chute_type != 'NORMAL':
				continue
	
			chute_info = self._dest_info(rec)
			orders = chute_info.get('orders') or []
			if not orders:
				continue
	
			for order_rec in orders:
				if not isinstance(order_rec, dict):
					continue
	
				order_number = str(order_rec.get('order_number') or '').strip()
				priority = str(order_rec.get('priority') or '').strip().upper()
	
				if not order_number:
					continue
	
				if priority not in ('1', '2', '3', '4', '5'):
					continue
	
				self._apply_priority_escalation(
					order_number,
					priority,
					allowed_chute_types=('NORMAL',),
				)
