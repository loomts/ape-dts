if (schema == "lua_test" and tb == "default_table")
then
    after["ccircle"] = '<(0,0),1>'
    after["ctime_tz"] = '10:20:12+00'
    after["cbool"] = 'true'
    after["cfloat8"] = '3.14768'
    after["cpolygon"] = '((0,0),(0,1),(1,1))'
    after["cbox"] = '(1,1),(0,0)'
    after["creal"] = '3.14'
    after["clseg"] = '[(0,0),(1,1)]'
    after["cnumeric"] = '1234.56'
    after["created_at"] = '2019-02-10 11:34:58'
    after["cuuid"] = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    after["ccidr"] = '192.168.1.0/24'
    after["cmoney"] = '$20.00'
    after["cinet"] = '192.168.1.100/32'
    after["cvarbit"] = '101'
    after["cline"] = '{0,1,2}'
    after["cvarchar"] = 'Test'
    after["cinteger"] = '2147483646'
    after["cmacaddr"] = '08:00:2b:01:02:03'
    after["ctext"] = 'hello world'
    after["cbits"] = '101'
    after["cxml"] = '<doc><item>abc</item></doc>'
    after["cpath"] = '((0,0),(0,1),(0,2))'
    after["val"] = '30.00000000'
    after["csmallint"] = '32766'
    after["cpoint"] = '(1,1)'
    after["cchar"] = 'a'
    after["cbigint"] = '9223372036854775806'
    after["cinterval"] = '01:02:03'
    after["cdate"] = '2019-02-01'
    after["cjson"] = '{"key": 123}'
    after["ctime"] = '10:20:11'
    after["created_at_tz"] = '2019-02-10 11:35:00+00'
end

if (schema == "lua_test" and tb == "numeric_table")
then
    after["r"] = '3.3'
    after["db_ninf"] = '-inf'
    after["r_pinf"] = 'inf'
    after["ss"] = '1'
    after["b"] = 'true'
    after["o"] = '4000000000'
    after["db_nan"] = 'NaN'
    after["db_int"] = '4'
    after["r_ninf"] = '-inf'
    after["si"] = '1'
    after["r_nan"] = 'NaN'
    after["bi"] = '1234567890123'
    after["i"] = '123456'
    after["db"] = '4.44'
    after["r_int"] = '3'
    after["bs"] = '123'
    after["db_pinf"] = 'inf'
end

if (schema == "lua_test" and tb == "numeric_decimal_table")
then
    after["d_int"] = '1.00'
    after["dvs_int"] = '10'
    after["d_nn"] = '3.30'
    after["d"] = '1.10'
    after["nvs"] = '22.2222'
    after["n_int"] = '22.0000'
    after["nzs_int"] = '22'
    after["dvs"] = '10.1111'
    after["nzs"] = '22'
    after["dzs_nan"] = 'NaN'
    after["nzs_nan"] = 'NaN'
    after["n"] = '22.2200'
    after["d_nan"] = 'NaN'
    after["n_nan"] = 'NaN'
    after["dvs_nan"] = 'NaN'
    after["dzs"] = '10'
    after["dzs_int"] = '10'
    after["nvs_int"] = '22'
    after["nvs_nan"] = 'NaN'
end

if (schema == "lua_test" and tb == "string_table")
then
    after["c"] = 'abc'
    after["ct"] = 'Hello World'
    after["vcv"] = 'bb'
    after["t"] = 'some text'
    after["bnn"] = '\\003\\004\\005'
    after["vc"] = 'žš'
    after["ch"] = 'cdef'
    after["b"] = '\\000\\001\\002'
end

if (schema == "lua_test" and tb == "network_address_table")
then
    after["i"] = '192.168.2.0/12'
end

if (schema == "lua_test" and tb == "cidr_network_address_table")
then
    after["i"] = '192.168.100.128/25'
end

if (schema == "lua_test" and tb == "macaddr_table")
then
    after["m"] = '08:00:2b:01:02:03'
end

if (schema == "lua_test" and tb == "cash_table")
then
    after["csh"] = '$1,234.11'
end

if (schema == "lua_test" and tb == "bitbin_table")
then
    after["bs7"] = '1000000'
    after["bv"] = '00'
    after["bvl"] = '1000000000000000000000000000000000000000000000000000000000000000'
    after["bvunlimited1"] = '101'
    after["bvunlimited2"] = '111011010001000110000001000000001'
    after["bv2"] = '000000110000001000000001'
    after["bs"] = '11'
    after["ba"] = '\\001\\002\\003'
    after["bol"] = '0'
    after["bol2"] = '1'
end

if (schema == "lua_test" and tb == "bytea_binmode_table")
then
    after["ba"] = '\\001\\002\\003'
end

if (schema == "lua_test" and tb == "time_table")
then
    after["tz_pinf"] = 'infinity'
    after["date"] = '2016-11-04'
    after["ts_ms"] = '2016-11-04 13:51:30.123'
    after["ts_large"] = '21016-11-04 13:51:30.123456'
    after["ts_ninf"] = '-infinity'
    after["tsneg"] = '1936-10-25 22:10:12.608'
    after["date_ninf"] = '-infinity'
    after["tz_large"] = '21016-11-04 06:51:30.123456+00'
    after["ttf"] = '24:00:00'
    after["tz_min"] = '4714-12-31 23:59:59.999999+00 BC'
    after["tz_max"] = '294247-01-01 23:59:59.999999+00'
    after["ts_large_us"] = '21016-11-04 13:51:30.123457'
    after["date_pinf"] = 'infinity'
    after["it"] = '1 year 2 mons 3 days 04:05:06.78'
    after["tip"] = '13:51:30.123'
    after["tz"] = '2016-11-04 11:51:30.123456+00'
    after["ttz"] = '13:51:30.123789+02'
    after["tsp"] = nil
    after["tz_ninf"] = '-infinity'
    after["ts_max"] = '294247-01-01 23:59:59.999999'
    after["tptz"] = '13:51:30.123+02'
    after["ts"] = '2016-11-04 13:51:30.123456'
    after["ts_large_ms"] = '21016-11-04 13:51:30.124'
    after["ts_us"] = '2016-11-04 13:51:30.123456'
    after["ti"] = '13:51:30'
    after["ts_min"] = '4713-12-31 23:59:59.999999 BC'
    after["ts_pinf"] = 'infinity'
end

if (schema == "lua_test" and tb == "text_table")
then
    after["jb"] = '{"bar": "baz"}'
    after["j"] = '{"bar": "baz"}'
    after["x"] = '<foo>bar</foo><foo>bar</foo>'
    after["u"] = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
end

if (schema == "lua_test" and tb == "geom_table")
then
    after["p"] = '(1,1)'
end

if (schema == "lua_test" and tb == "range_table")
then
    after["unbounded_exclusive_tsrange"] = '[2019-03-31 15:30:00,infinity)'
    after["unbounded_exclusive_daterange"] = '[2019-03-31,infinity)'
    after["bounded_inclusive_tstzrange"] = '[2017-06-05 11:29:12.549426+00,2017-06-05 12:34:56.789012+00]'
    after["int8_number_range"] = '[1000000,6000000)'
    after["unbounded_exclusive_tstzrange"] = '[2017-06-05 11:29:12.549426+00,)'
    after["bounded_exclusive_daterange"] = '[2019-03-31,2019-04-30)'
    after["bounded_inclusive_tsrange"] = '[2019-03-31 15:30:00,2019-04-30 15:30:00]'
    after["int4_number_range"] = '[1000,6000)'
    after["numerange"] = '[5.3,6.3)'
end

if (schema == "lua_test" and tb == "array_table")
then
    after["text_array"] = '{one,two,three}'
    after["bigint_array"] = '{1550166368505037572}'
    after["tstzrange_array"] = '{"[2017-06-05 11:29:12.549426+00,)","[2017-06-05 11:29:12.549426+00,2017-06-05 12:34:56.789012+00]"}'
    after["daterange_array"] = '{"[2019-03-31,infinity)","[2019-03-31,2019-04-30)"}'
    after["oid_array"] = '{3,4000000000}'
    after["numeric_array"] = '{1.20,3.40,5.60}'
    after["macaddr_array"] = '{08:00:2b:01:02:03,08:00:2b:01:02:03,08:00:2b:01:02:03}'
    after["int_array"] = '{1,2,3}'
    after["inet_array"] = '{192.168.2.0/12,192.168.1.1,192.168.0.2/1}'
    after["json_array"] = '{"{\\\"bar\\\": \\\"baz\\\"}","{\\\"foo\\\": \\\"qux\\\"}"}'
    after["numerange_array"] = '{"[5.3,6.3)","[10.0,20.0)"}'
    after["citext_array"] = '{four,five,six}'
    after["cidr_array"] = '{192.168.100.128/25,192.168.0.0/25,192.168.1.0/24}'
    after["date_array"] = '{2016-11-04,2016-11-05,2016-11-06}'
    after["char_array"] = '{"cone      ","ctwo      ","cthree    "}'
    after["jsonb_array"] = '{"{\\\"bar\\\": \\\"baz\\\"}","{\\\"foo\\\": \\\"qux\\\"}"}'
    after["int4range_array"] = '{"[1,6)","[1,4)"}'
    after["int8range_array"] = '{"[1000000,6000000)","[5000,9000)"}'
    after["varchar_array"] = '{vcone,vctwo,vcthree}'
    after["tsrange_array"] = '{"[2019-03-31 15:30:00,infinity)","[2019-03-31 15:30:00,2019-04-30 15:30:00]"}'
    after["varnumeric_array"] = '{1.1,2.22,3.333}'
    after["uuid_array"] = '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,f0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'
end

if (schema == "lua_test" and tb == "custom_table")
then
    after["lt"] = 'Top.Collections.Pictures.Astronomy.Galaxies'
    after["lt_array"] = '{Ship.Frigate,Ship.Destroyer}'
    after["i"] = '0-393-04002-X'
    after["n"] = nil
end

if (schema == "lua_test" and tb == "hstore_table")
then
    after["hs"] = '"key"=>"val"'
end

if (schema == "lua_test" and tb == "hstore_table_mul")
then
    after["hsarr"] = '{\"\\\"key4\\\"=>\\\"val4\\\", \\\"key5\\\"=>NULL\",\"\\\"key6\\\"=>\\\"val6\\\"\"}'
    after["hs"] = '"key1"=>"val1", "key2"=>"val2", "key3"=>"val3"'
end

if (schema == "lua_test" and tb == "hstore_table_with_special")
then
    after["hs"] = '"key 2"=>" ##123 78", "key_#1"=>"val 1"'
end

if (schema == "lua_test" and tb == "circle_table")
then
    after["ccircle"] = '<(10,20),10>'
end

if (schema == "lua_test" and tb == "macaddr8_table")
then
    after["m"] = '08:00:2b:01:02:03:04:05'
end

if (schema == "lua_test" and tb == "postgis_table")
then
    after["ml"] = '0105000020E610000001000000010200000002000000A779C7293A2465400B462575025A46C0C66D3480B7FC6440C3D32B65195246C0'
    after["p"] = '0101000020730C00001C7C613255DE6540787AA52C435C42C0'
end

if (schema == "lua_test" and tb == "postgis_array_table")
then
    after["gann"] = '{010700000000000000:01030000000100000005000000B81E85EB51D0644052B81E85EB5147C0713D0AD7A350664052B81E85EB5147C0713D0AD7A35066409A999999993941C0B81E85EB51D064409A999999993941C0B81E85EB51D0644052B81E85EB5147C0}'
    after["ga"] = '{010700000000000000:01030000000100000005000000B81E85EB51D0644052B81E85EB5147C0713D0AD7A350664052B81E85EB5147C0713D0AD7A35066409A999999993941C0B81E85EB51D064409A999999993941C0B81E85EB51D0644052B81E85EB5147C0}'
end

if (schema == "lua_test" and tb == "timezone_table")
then
    after["t4"] = '2024-05-09 07:55:40.372424+00'
    after["t3"] = '2024-05-09 07:55:40.372424'
    after["t2"] = '07:55:40.372424+00'
    after["t1"] = '07:55:40.372424'
end

if (schema == "lua_test" and tb == "col_has_special_character_table")
then
    after["col`1"] = 'col:1:value'
    after["col,2"] = 'col&2:value'
    after["col\\3"] = 'col\\3:value'
end


print("schema: "..schema)
print("tb: "..tb)
print("row_type: "..row_type)

print("")
print("before")
for k, v in pairs(before) do
    print(k, v)
end

print("")
print("after")
for k, v in pairs(after) do
    print(k, v)
end