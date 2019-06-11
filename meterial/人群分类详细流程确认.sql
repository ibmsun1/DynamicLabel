
0.数据
		|.券敏感: 	  ods_vip_behavior1
		|.积分敏感:   ods_vip_behavior2
		|.互动意愿:   ods_vip_behavior3
		|.中间表打分: dw_vip_scores 
		|.聚类结果:	  dw_vip_results		
		|.人群分类:   dw_vip_labels
		

1.表结构
	
# 主题 1 表结构		--券敏感型 
CREATE TABLE `ods_vip_behavior1`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `acttype` int,
  `couponrate` double,
  `couponcount` int,
  `avgtimelength ` double,
  `lastdistancecurrentdays` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
with serdeproperties('serialization.null.format' = '')
STORED AS ORC;

	
# 主题 2 表结构		--积分敏感型 
CREATE TABLE `ods_vip_behavior2`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `acttype` int,
  `count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
with serdeproperties('serialization.null.format' = '')  
STORED AS ORC;

																		
# 主题 3 表结构		--互动意愿型 
CREATE TABLE `ods_vip_behavior3`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `acttype` int,
  `count` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
with serdeproperties('serialization.null.format' = '')  
STORED AS ORC;


# 中间表  表结构	-- 数据处理 打分, 分数用于聚类.
CREATE TABLE `dw_vip_scores`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `score` double,
  `subject` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
with serdeproperties('serialization.null.format' = '')
STORED AS ORC;


#.中间表  表结构	-- 数据处理 行为整合, 构造行为字段 score :{acttype:score;acttype:score;acttype:score...}.
CREATE TABLE `dw_vip_behaviors`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `score` double,
  `subject`  int,
  `behavior` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
with serdeproperties('serialization.null.format' = '')
STORED AS ORC;


#.中间表  表结构	-- 聚类结果表, 用于决策树分析 定位人群得分区间.	
CREATE TABLE `dw_vip_results`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `score` double,
  `center` int,
  `subject` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
with serdeproperties('serialization.null.format' = '')
STORED AS ORC;


# 结果表  表结构	-- 人群标签结果表
CREATE TABLE `dw_vip_labels`(
  `vipid` bigint,
  `brandid` int,
  `copid` int,
  `score` double,
  `center` int,
  `subject`  int,
  `behavior` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
with serdeproperties('serialization.null.format' = '')
STORED AS ORC;

# 联系导购新增中间表
CREATE TABLE `crm_shop_guide_merge`(
  `vipid` bigint,
  `brandid` int,
  `shopid` int,
  `salerid` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
with serdeproperties('serialization.null.format' = '')
STORED AS ORC;


动态标签  行为权重配置

CREATE TABLE `opt_bi_dynamiclable_cfg` (
  `Id` bigint(20) NOT NULL COMMENT '编号',
  `CopId` int(11) DEFAULT NULL COMMENT '所属集团',
  `BrandId` int(11) NOT NULL DEFAULT '0' COMMENT '所属品牌',
  `Subject` int(11) DEFAULT NULL COMMENT '主题编号, 1券敏感, 2积分敏感, 3互动意愿',
  `Behavior` varchar(16) DEFAULT 'Ezr' COMMENT '行为, 完善资料, 游戏, 积分对券, 积分兑礼, 大转盘, 砸金蛋, 刮刮卡, 预约, 到店, 领券, 领红包, 分享红包, 打开红包, 联系导购, 订单评价',
  `Weight` double COMMENT '权重',
  `CreateUser` varchar(16) DEFAULT 'Ezr' COMMENT '创建人',
  `CreateDate` datetime DEFAULT NULL COMMENT '创建时间',
  `LastModifiedUser` varchar(16) DEFAULT 'Ezr' COMMENT '最后修改人',
  `LastModifyDate` datetime DEFAULT NULL COMMENT '最后更新时间',
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `opt_bi_dynamiclable_cfg` (
  `Id` bigint(20) NOT NULL COMMENT '编号',
  `CopId` int(11) DEFAULT NULL COMMENT '所属集团',
  `BrandId` int(11) NOT NULL DEFAULT '0' COMMENT '所属品牌',
  `Subject` int(11) DEFAULT NULL COMMENT '主题编号, 1表示券敏感, 2表示积分敏感, 3表示互动意愿',
  `TypeId` int(11) DEFAULT NULL COMMENT '行为类别编号, 1完善资料, 2互动游戏, 3积分对券, 4积分兑礼, 5大转盘, 6砸金蛋, 7',
  `WeightList` double DEFAULT '0' COMMENT '完善资料',
  `CreateUser` varchar(16) DEFAULT 'Ezr' COMMENT '创建人',
  `CreateDate` datetime DEFAULT NULL COMMENT '创建时间',
  `LastModifiedUser` varchar(16) DEFAULT 'Ezr' COMMENT '最后修改人',
  `LastModifyDate` datetime DEFAULT NULL COMMENT '最后更新时间',
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;