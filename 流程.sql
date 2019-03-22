
一、目标：基于用户行为打分,  通过划分聚类将人群分类,	以分数区间来定位人群标签
	
二、实现：

	0.数据
		|.券敏感: 	  ods_vip_behavior1
		|.积分敏感:   ods_vip_behavior2
		|.互动意愿:   ods_vip_behavior3
		|.中间表打分: dw_vip_scores 
		|.聚类结果:	  dw_vip_results		
		|.人群分类:   dw_vip_labels
		
	1.数据处理: etl和打分.	-spark job
	
		|.主题 1   
			核销率[%]	核销券数	核销券均核销时长[Double]	 最后一次核销距离当天时长[-1]
				A			B				C					D
				
			Ⅰ.A -> 去掉%, 取整数部分, 将0-1区间的数归到1: C < (0, 1) ->  1,	D < (0, 1) ->  1, 处理-1的数据D[-1] -> 0.0;
			
			Ⅱ.取[vipid, A], [vipid, B], [vipid, C], [vipid, D], 做特征缩放, 使A,B,C,D <[1, 100], 以方便计算得分
			
			Ⅳ.针对归一后数据打分, 打分公式是基于雷达图面积作为得分, 由于C和D对于积分敏感度是负激励,另依据雷达图面积的定义, 
				故: score = (A` + B`) * (1 / C` + 1/ D`), 结果按主题写入[(vipid, score, subject) -> dw_vip_scores]
			
		|.主题 2、3
		
			Ⅰ.根据shell传参, 获取主题对应的行为: subject -> List[acttype]
			
			Ⅱ.List[acttype] -> sql + weightTuples, 通过行为列表 
			
				获取 sql -> "select vipid, name, brandid, copid, acttype, count from " + Utils.sourceDataPointsAndInteractive + " " + "where acttype in " + "(" + actType + ")"
				
				权重组合: typeTuples = (complMater, game, vouchers, eli, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
						  typeTuples = (0.0, 0.0, 0.0, 0.0, bigWheel, goldenEggs, scratch, complmaterial, reservationnum, storenum, couponnum, redenvelopesnum, sharetimes, opentimes, conshopguide, orderevaluate)
			
			Ⅳ.归一[mn, mx], 保证不同行为下数据一致性, 取到样本的最小值sample.min -> mn, 最大值sample.max -> mx, 做特征缩放处理
			
			Ⅵ.针对归一后数据打分,打分公式是基于不同行为得分的权重求和, 
				即: score = sum(count` * weight), 结果按主题写入[(vipid, score, subject) -> dw_vip_scores]
			
	2.数据分析	[数据分布、以及确定合适的K]	-jupyter
	
		|.主题 1 -相关性分析, 是否能够对数据做降维处理,  画出数据的散点图, 了解数据分布
			|.变量分布, 数据大致分布是什么样的
				plt.hist(data.iloc[:, i], bins=20, normed=True)
				
			|.A, B, C, D 四个行为维度数据做描述性分析
			
				data[i].describe() 	
				count    1.773440e+06	
				mean     4.698072e-01	
				std      2.852022e-01
				min      0.000000e+00
				25%      2.000000e-01
				50%      5.000000e-01
				75%      7.000000e-01
				max      1.000000e+00
				Name: 0, dtype: float64

			|.两两关联程度图, 看两两间的相关性
				sns.pairplot(data)
		
		|.主题 1、2、3
		
			|.K - SSE[聚类中心数, 簇内方差] 图: 根据折线图的趋势, SSE下降趋势转折最大的点, 初步确定K数
			
			|.	K - Silhouette Coefficient -> 轮廓系数图: 根据Silhouette Coefficient的定义，值较大时的K较优，所以估算最优K=8为最优点
		
			|.	K - Cluster label -> 轮廓宽度图: 根据不同K的轮廓宽度图含义, 轮廓宽度相近程度, 判断所选K数是否合理
		
	3.聚类			- spark job
		|.取样: "select vipid, score from " + Utils.clusterData + s" where subject = $subject "
		|.聚类 -> [transDF, centers]
		
	4.数据整理		-spark job
		
		|.transDF[RDD(vipid, score, classification)] 
		
		|.behaviour[RDD(vipid, behaviour:"acttype:score`")]
		
		|.sample[RDD(vipid,brandid, copid, name)]
		
		|.RDD[vipId, brandId, copId, name, score/(List.length), classification, subject, behaviour]
		
		|.writeDF.write.mode(SaveMode.Append).insertInto(Utils.native)

三、分析与总结
		|.虽然结已经出来, 但是人群分类的准确性还需要进一步验证	
		|.需要对优质人群做进一步分析