{% macro mmsi_format_class(column_name) %}

    multiIf(

        length(toString({{ column_name }})) != 9,
        'invalid_length',

        startsWith(toString({{ column_name }}), '00'),
        'coast_station',

        startsWith(toString({{ column_name }}), '0'),
        'group_call',

        startsWith(toString({{ column_name }}), '111'),
        'sar_aircraft',

        startsWith(toString({{ column_name }}), '99'),
        'aid_to_navigation',

        startsWith(toString({{ column_name }}), '98'),
        'parent_ship_craft',

        startsWith(toString({{ column_name }}), '970'),
        'ais_sart',

        startsWith(toString({{ column_name }}), '972'),
        'mob_device',

        startsWith(toString({{ column_name }}), '974'),
        'epirb_ais',

        startsWith(toString({{ column_name }}), '8'),
        'handheld_vhf',

        substring(toString({{ column_name }}), 1, 1)
            IN ('2', '3', '4', '5', '6', '7'),
        'ship',

        'nonstandard'

    )

{% endmacro %}