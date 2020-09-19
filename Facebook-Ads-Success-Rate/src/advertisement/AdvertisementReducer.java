package advertisement;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reduce has to do:
 * 1) Calculate succes rate of each record
 * 2) Average it
 * */
public class AdvertisementReducer extends Reducer<Text, Text, Text, Text>
{
	//Ecommerce    [ { Dallas ,39,13 }  Dallas,281,5 } {Austin,341,9} {Austin,398,10}................]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
    {

    	//Used to store success rate corresponding to city
    	//key: city value:total_success_rate, count
    	//Example: 
    	//City successRate and totalCount
    	//Ecommerce    [ { Dallas,39,13 }  {Dallas,281,5 } {Austin,341,9} 
    	HashMap<String, String> citySuccessRate = new HashMap<String, String>();


    	Iterator<Text> itr = values.iterator();	
    	//Will iterate through: Ecommerce    [ { Dallas ,39,13 }  Dallas,281,5 } {Austin,341,9} {Austin,398,10}...
    	//The iterator will go through all values of the key
    	while (itr.hasNext())
    	{
    		String value = itr.next().toString();          // value =  Dallas,281,5

    		String[] adInformation = value.split(",");     //adInformation = [ {Dallas} {281} {5}]

    		String City = adInformation[0].trim();         //city = Dallas

    		int clickCount = Integer.parseInt(adInformation[1]);   // clickCount= 281

    		int conversionCount = Integer.parseInt(adInformation[2]);   //sales = 5 

    		//To get in terms of percetange
    		Double succRate = new Double(conversionCount/(clickCount*1.0)*100);   // succRate = 1.77

    		//Checking if the hashmap contains key
    		if (citySuccessRate.containsKey(City)) 
    		{
    			//City exists in hashmap, so we update it
    			
    			//Get value from city
    			String val = citySuccessRate.get(City);    // val =  33.3, 1
    			
    			//split value into array of strings
    			String[] city_value = val.split(",");         // city_value = [ {33.3} {1} ]      
    			
    			//Add passed on successrate to the total success rate
    			// totalSuccRate  = (33.3 + 1.77) = 35.07
    			Double totalSuccRate = Double.parseDouble(city_value[0]) + succRate;     
    			
    			//increase the total count of sucesses by 1
    			int totalCount = Integer.parseInt(city_value[1]) + 1; 

    			//UPDATE current key with new total success rate and total count
    			citySuccessRate.put(City, totalSuccRate + "," + totalCount);

    		}else 
    		{
    			//when City DNE, CREATE city and successrate and +1
    			citySuccessRate.put(City, succRate + ",1");
    		}
    	}
	
		System.out.println(citySuccessRate.toString());
		
		//We will iterate through hashamp that contains the city with final successrate and
		//final total count
		//ex:
		// city_key = Dallas  value = "65.07,3"
		for (Map.Entry<String, String> city_key : citySuccessRate.entrySet())        
		{
			// final_city_success_rate_values = [{65.07} {3}]
			String[] final_city_success_rate_values = city_key.getValue().split(",");     
			
			//Convert to floats and divided to get average percentage success rate per city
			Double avgSccRate = Double.parseDouble(final_city_success_rate_values[0])/Integer.parseInt(final_city_success_rate_values[1]);
			
			//write to final object the city and average percentage success rate per city
			c.write(key, new Text(city_key.getKey() + "," + avgSccRate));
		}
    }
}