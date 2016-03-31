-- 商户推广统计(存储过程)
-- by guoshijie
-- update_time: 2015-12-15 13:07:36

delimiter $

drop procedure if exists pro_shop_share$

create procedure pro_shop_share()
begin

declare shop_id int(11);

declare scan_count int(11);
declare scan_android_count int(11);
declare scan_ios_count int(11);

declare red_count int(11);
declare red_android_count int(11);
declare red_ios_count int(11);

declare real_count int(11);
declare percent_count decimal(12,4);

declare scene_one_count int(11);
declare scene_two_count int(11);
declare scene_three_count int(11);
declare scene_four_count int(11);

declare scene_one_scan_count int(11);
declare scene_two_scan_count int(11);
declare scene_three_scan_count int(11);
declare scene_four_scan_count int(11);

declare red_packet_percent_count decimal(12,4);
declare dur_days int(11);
declare avg_real_count int(11);
declare avg_scan_count int(11);

declare duobao_sum_count int(11);
declare duobao_item_count int(11);

-- 这个用于处理游标到达最后一行的情况
DECLARE s int default 0;

DECLARE cursor_name CURSOR FOR select id from sh_shop where enabled = 1;

-- 设置一个终止标记
DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET s=1;
-- DECLARE CONTINUE HANDLER FOR NOT FOUND SET s=1;

-- 打开游标
OPEN cursor_name;
fetch  cursor_name into shop_id;

while s <> 1 do
    SELECT count(*) into scan_count FROM sh_shop_scan where sh_shop_id = shop_id;
    SELECT count(*) into scan_android_count FROM sh_shop_scan where sh_shop_id = shop_id and os_type=1;
    SELECT count(*) into scan_ios_count FROM sh_shop_scan where sh_shop_id = shop_id and os_type=2;

    select count(*) into red_count from sh_shop_tel where sh_shop_id=shop_id;
    select count(*) into red_android_count from sh_shop_tel a inner join sh_red_packet b on a.sh_user_tel=b.tel where a.sh_shop_id=shop_id and b.os_type=1 and b.channel=7;
    select count(*) into red_ios_count from sh_shop_tel a inner join sh_red_packet b on a.sh_user_tel=b.tel where a.sh_shop_id=shop_id and b.os_type=2 and b.channel=7;

    select count(*) into real_count from sh_shop_tel a inner join sh_user b on a.sh_user_tel = b.tel where sh_shop_id=shop_id and b.is_real=1;

    set percent_count := real_count / scan_count;

    select count(*) into scene_one_count from sh_shop_tel where sh_shop_id=shop_id and scene=1;
    select count(*) into scene_two_count from sh_shop_tel where sh_shop_id=shop_id and scene=2;
    select count(*) into scene_three_count from sh_shop_tel where sh_shop_id=shop_id and scene=3;
    select count(*) into scene_four_count from sh_shop_tel where sh_shop_id=shop_id and scene=4;

    select count(*) into scene_one_scan_count from sh_shop_scan where sh_shop_id=shop_id and scene=1;
    select count(*) into scene_two_scan_count from sh_shop_scan where sh_shop_id=shop_id and scene=2;
    select count(*) into scene_three_scan_count from sh_shop_scan where sh_shop_id=shop_id and scene=3;
    select count(*) into scene_four_scan_count from sh_shop_scan where sh_shop_id=shop_id and scene=4;
    set red_packet_percent_count := red_count / scan_count;

    #累计推广天数
    SELECT TIMESTAMPDIFF(DAY,(select insert_date from sh_shop_share where sh_shop_id = shop_id order by insert_date asc limit 1),(select insert_date from sh_shop_share where sh_shop_id = shop_id order by insert_date desc limit 1)) into dur_days;

    #平均每天注册人数
    if(dur_days=0) then
        set avg_real_count := 0;
        set avg_scan_count := 0;
    else
        set avg_real_count := real_count / dur_days;
        set avg_scan_count := scan_count / dur_days;
    end if;

    #累计夺宝人次
    select duobao_sum into duobao_sum_count from sh_shop where id = shop_id;

    #入库
    insert into sh_shop_share(sh_shop_id,scan_sum,scan_sum_android,scan_sum_ios,avg_scan_sum,scan_num,red_packet_android,red_packet_ios,real_num,percent,
        scene_one,scene_two,scene_three,scene_four,insert_date,scene_one_scan,scene_two_scan,scene_three_scan,scene_four_scan,
        red_packet_percent,days,avg_real_num,duobao_sum)
    values(shop_id,scan_count,scan_android_count,scan_ios_count,avg_scan_count,red_count,red_android_count,red_ios_count,real_count,ifnull(percent_count,0),
        scene_one_count,scene_two_count,scene_three_count,scene_four_count,now(),scene_one_scan_count,scene_two_scan_count,scene_three_scan_count,scene_four_scan_count,
        ifnull(red_packet_percent_count,0),dur_days,avg_real_count,duobao_sum_count);

    #读取下一行的数据
    fetch  cursor_name into shop_id;

end while;

#关闭游标
CLOSE cursor_name ;

end$

delimiter ;

call pro_shop_share();

-- 查看数据库是否开启事件支持
show VARIABLES LIKE '%sche%';
SET GLOBAL event_scheduler = 1;

-- 创建事件的定时器
DROP EVENT IF EXISTS event_pro_shop_share;

CREATE EVENT IF NOT EXISTS event_pro_shop_share
ON SCHEDULE every 5 minute
on completion preserve enable
DO CALL pro_shop_share;

-- 开启事件test_event
alter event event_pro_shop_share on completion preserve enable;
-- 关闭事件test_event
alter event event_pro_shop_share on completion preserve disable;

-- 查看已经创建的事件
select * from  mysql.event;
