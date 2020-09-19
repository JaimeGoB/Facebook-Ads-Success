# Facebook-Ads-Success

In this program I will analyze data from Facebook Advertisment using **Map-Reduce**. In order to predict the success rate of advertisement per Industry and per City. 

Attached is a description of the dataset:

- Advertisment ID
- Data
- City
- Industry
- Clicks
- Sales
- AgeGroup

![image 1](https://github.com/JaimeGoB/Facebook-Ads-Success/blob/master/Facebook-Ads-Success-Rate/src/data/input.png)


## Mapper
The mapper will parse text input file line. Ex of reading line 1 from file:

&emsp; FKLY490998LB,2009-12-29 06:12:17,Dallas,Ecommerce,39,13,25-35
  
It will pass the following key value pair to reducer: 

&emsp; KEY: Ecommerce VALUE: Dallas, 39, 13  


## Reducer 
From the input file I will calculate final success rate (percentage) per city and industry by the following example:

##### Step 1) Calculate Average succes per city

Row 1 Dallas - Ecommerce  (sales) / (clicks) -> success_rate 1 
Row 2 Dallas - Ecommerce  (sales) / (clicks) -> success_rate 2
........
Row n Dallas - Ecommerce  (sales) / (clicks) -> success_rate n

##### Step 2) Calculate Average success for all cities

final success rate (percentage) per city and industry =
(success_rate 1)  + (success_rate 2 ) + ... + (success_rate n). / (n rows with Ecommerce in City c)

## Final output file the my Map-Reduce algorithm:

![image 1](https://github.com/JaimeGoB/Facebook-Ads-Success/blob/master/Facebook-Ads-Success-Rate/src/data/output.png)
