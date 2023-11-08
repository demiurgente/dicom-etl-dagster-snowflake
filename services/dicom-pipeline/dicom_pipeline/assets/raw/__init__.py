from .lib import COLUMN_PARTITION_MAP, RawConfig, read_dicom_file_to_df

from . import devices as devices
from . import images as images
from . import patients as patients
from . import series as series
from . import studies as studies

COLUMN_MAP = {
	'devices': devices.columns,
	'images': images.columns,
	'patients': patients.columns,
	'series': series.columns,
	'studies': studies.columns,
}

from .devices import devices
from .images import images
from .patients import patients
from .series import series
from .studies import studies