
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import random


def generate_data():

    """
    This is a utility function to generate sample data
    """
    main_list=[]  #holds the outer list
    count_non_legal=0
    expected_non_legal=10    #number of deliveries that are not legal
    total_deliveries=300   #number of total deliveries
    delivery_type_arr=['legal','nb','wd']  #type of deliveries

    for i in range(1,(total_deliveries+1+expected_non_legal)):

        if(count_non_legal>=expected_non_legal):   #we only generate legal balls if count of non legal > required count
            delivery_type_arr=['legal']

        delivery_type=random.choice(delivery_type_arr)

        if(delivery_type =='nb' or delivery_type=='wd'):
            count_non_legal=count_non_legal+1

        sub_list=[random.randint(1,6),delivery_type]
        main_list.append(sub_list)


    #while(main_list[len(main_list)-1][1] in ['wd','nd']):    #had written this to avoid the fact tha random shuffling can sometimes cause nb or wd at the end (incorrect)
    random.shuffle(main_list)


    for idx,val in enumerate(main_list):  #adding in delivery ids after shuffling
        val.insert(0,idx+1)

    return main_list


def generate_grouping_col(l):

    """
    Function to generate a unique grouping column based on type of delivery
    """
    counter = 1   #over number for grouping
    i = 0

    while (i < len(l)):
        j = i
        usual_group = 6
        while (j < i + usual_group):   #for each i we go for 6 records after it(usually)-- in case we encounter wd/nb we increase the loop boundary to go over many items

            if (l[j][2] == 'wd' or l[j][2] == 'nb'):

                usual_group = usual_group + 1

            l[j].insert(3, counter)   #adding in the over number
            j = j + 1

        counter = counter + 1
        i = i + usual_group     #if we have 1 nb for example we should not consider that in the next over

    return l



spark = SparkSession.builder.appName("s").enableHiveSupport().getOrCreate()


legal_arr=['nb','wd']

col = ['delivery_no', 'runs_scored', 'delivery_type']

jobs = spark.createDataFrame(generate_data(), col)

jobs.show(500,truncate=False)


jobs_with_score_handled=\
    jobs.withColumn("runs_scored",when((column('delivery_type') == 'nb') | (column('delivery_type')== 'wd') ,column('runs_scored')+1).otherwise(column('runs_scored')))\
    .select(collect_list(array(column('delivery_no'),column('runs_scored'),column('delivery_type'))).alias("all_data"))  #increase runs for nb or wd

spark.udf.register("my_udf",generate_grouping_col,ArrayType(ArrayType(StringType())))

x=jobs_with_score_handled.withColumn("group_col",expr("my_udf(all_data)")).select(explode("group_col").alias("_added")).\
    select(expr("_added[0] delivery_id"),expr("_added[1] runs_scored"),expr("_added[2] delivery_type"),expr("_added[3] over_number"))

final_df=x.groupby("over_number").agg(sum("runs_scored").alias("runs_scored")) #group by over number and then sum

final_df.orderBy(column("over_number").cast('int')).show(100,truncate=False)



