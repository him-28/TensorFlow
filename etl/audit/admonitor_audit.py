#encoding=utf-8

from etl.util.csvvalidator import CSVValidator, enumeration, number_range_inclusive,\
        write_problems, datetime_string, RecordError

def admonitor_csv_validator():
    """Create AD Monitor CSV validator for data"""
    field_names = (
            'ip',
            'province',
            'city',
            'ad_event_type',
            'url',
            'video_id',
            'playlist_id',
            'board_id',
            'request_res',
            'ad_list',
            'time_delay',
            'request_str',
            'slot_id',
            'compaign_id',
            'creator_id',

            )


