package advertisement;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//Id		   			 ,date    , City, Sector   ,clicks, sales, age-group
//FKLY490998LB,2009-12-29 06:12:17,Dallas,Ecommerce,39,13,25-35
public class AdvertisementMapper extends Mapper<LongWritable, Text, Text, Text>
{

	private Text Sector = new Text();
	private Text advertisementInformation = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
	{

		String line = value.toString();
		//Splitting line by "," and storing entire line into array
		//        Id		   		date             City     Sector      clicks, sales, age-group
		String[] ad = line.split(",");        
		
		// ad contains the follwing:
		//  [ {FKLY490998LB} {2009-12-29 06:12:17} {Dallas}  {Ecommerce}   {39}    {13}   {25-35} ]
		//ad[        0                 1               2          3          4       5        6   ]
		
		//Ecommerce
		Sector.set(ad[3]);
		//			                  Dallas        39             13  
		advertisementInformation.set(ad[2] + "," + ad[4] + "," +  ad[5]);
		
		// KEY: Ecommerce VALUE: Dallas, 39, 13  
		c.write(Sector, advertisementInformation);
	}     
}
