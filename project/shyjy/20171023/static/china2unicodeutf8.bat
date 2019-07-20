set root=%cd%
@echo
c:
cd %JAVA_HOME%\bin
native2ascii -encoding utf-8 %root%/traffic_bi_id_catch.txt %root%/traffic_bi_id_catch_unicode.txt
native2ascii -encoding utf-8 %root%/traffic_bi_static_host.txt %root%/traffic_bi_static_host_unicode.txt
native2ascii -encoding utf-8 %root%/traffic_bi_static_phone_head.txt %root%/traffic_bi_static_phone_head_unicode.txt
native2ascii -encoding utf-8 %root%/traffic_bi_static_terminal_detail.txt %root%/traffic_bi_static_terminal_detail_unicode.txt
native2ascii -encoding utf-8 %root%/traffic_bi_static_ua.txt %root%/traffic_bi_static_ua_unicode.txt