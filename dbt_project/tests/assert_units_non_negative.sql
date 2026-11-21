-- 第 2 關小挑戰的「第二道防線」：
-- Silver 已經擋掉負數了，但 Gold 再檢查一次，萬一哪天有人改壞 Silver 規則，這裡會抓到
select * from {{ ref('fct_machine_hourly') }} where units_produced < 0
