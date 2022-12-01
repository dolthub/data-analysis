# NIMBY vs. YIMBY: outlier analysis of new construction projects in USA

During the second iteration of DoltHub's USA housing price data bounty a large amount of public real estate data was scraped, wrangled and imported into a version-controlled database. This enables us to do some exploration and analysis for the purpose of gaining insight into the dynamics of real estate market. Some parts of United States are said to suffer from NIMBYism - a resistance to new property developments in area. One famous example is Marc Andreesen, a prominent Silicon Valley venture capitalist, going out of his way to prevent new housing to be built in his town - Atherton, CA. But perhaps there's also areas that welcome and support new real estate projects? By wielding the power of programming and open data, we are able to leverage the `us-housing-prices-v2` database and find which are which. 

Our approach to data analysis approach is going to be as follows. We are going to limit data being analysed to timeframe from 2009 June 30 to 2020 January 1. This will provide us 10.5 years worth of data from the times between the official end of Great Recession of 2008 to the very beginning of the current quite complicated decade. Thus we will be looking into steady-state trends from relatively recent past period that had no major shocks to the entire real estate market. Furthermore, we are going to narrow our view into the initial records of property being sold, which implies that a property is newly built and just entered the market. We are going to count such initial sales for each county represented in the database. Some counties are geographically large, some are populous, some are small in area and/or population. To accomplish an apples to apples comparison we are going to normalise number of initial sales by population and by land area. Lastly, we are going to compute standard deviation values for each county from per capita and per area values to appreciate how much they stand out.

Property sales data was primarily sources from the official county institution websites. Due to scale and complexity of the data landscape the gathered dataset is unfortunately quite sparse - only 417 counties and county-equivalent territories out of over 3000 are represented in the database for the time window we're looking into. This makes a general analysis for the entire USA impossible, but we're looking for places that stand out in terms of how little or how much new property objects are entering the market. Thus we will further narrow down the results to shortlist such outliers.


```python
from io import StringIO

import mysql.connector as connection
from sqlalchemy import create_engine
import pandas as pd
import requests
```

We are going to use [US counties dataset](https://www.openintro.org/data/index.php?data=county_complete) provided by OpenIntro to enrich data scraped during bounty with population and land area numbers so that we could computer per capita and per area values. Once that is done, we are ready to crunch the numbers.


```python
resp = requests.get("https://www.openintro.org/data/csv/county_complete.csv", 
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"})
resp
```




    <Response [200]>




```python
def cleanup_county(c):
    if c is None:
        return None
    
    c = c.upper()
    
    to_remove = [
        "COUNTY",
        "CITY",
        "TOWN",
        "'",
        ",",
        " OF"
    ]
    
    for tr in to_remove:
        c = c.replace(tr, "")
        
    c = c.strip()
    
    return c
    
def fix_fips(fips):
    if type(fips) == int:
        fips = str(fips)
        
    if len(fips) == 4:
        fips = '0' + fips
    
    return fips
    
buf = StringIO(resp.text)

df_counties = pd.read_csv(buf, dtype={'fips': str})
df_counties = df_counties[["fips", "state", "name", "pop2010", "area_2010", "density_2010"]]
df_counties = df_counties.rename(columns={'name': 'county'})
df_counties['county'] = df_counties['county'].apply(cleanup_county)
df_counties['fips'] = df_counties['fips'].apply(fix_fips)

db_connection_str = 'mysql+mysqlconnector://rl:trustno1@localhost/us_housing_prices_v2'
db_connection = create_engine(db_connection_str)

query = "SELECT * FROM `states`;"
states_df = pd.read_sql(query, db_connection)
states_df = states_df.rename(columns={'name': 'state'})

df_counties = pd.merge(df_counties, states_df, on='state')
df_counties['county_state'] = df_counties['county'] + ', ' + df_counties['code'] 
del df_counties['county']
del df_counties['code']
del df_counties['state']

df_counties.loc[df_counties['fips'] == "51600", "county_state"] = "FAIRFAX CITY, VA"
df_counties.loc[df_counties['fips'] == "24510", "county_state"] = "BALTIMORE CITY, MD"

df_counties
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>pop2010</th>
      <th>area_2010</th>
      <th>density_2010</th>
      <th>county_state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>01001</td>
      <td>54571</td>
      <td>594.44</td>
      <td>91.8</td>
      <td>AUTAUGA, AL</td>
    </tr>
    <tr>
      <th>1</th>
      <td>01003</td>
      <td>182265</td>
      <td>1589.78</td>
      <td>114.6</td>
      <td>BALDWIN, AL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01005</td>
      <td>27457</td>
      <td>884.88</td>
      <td>31.0</td>
      <td>BARBOUR, AL</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01007</td>
      <td>22915</td>
      <td>622.58</td>
      <td>36.8</td>
      <td>BIBB, AL</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01009</td>
      <td>57322</td>
      <td>644.78</td>
      <td>88.9</td>
      <td>BLOUNT, AL</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3137</th>
      <td>56037</td>
      <td>43806</td>
      <td>10426.65</td>
      <td>4.2</td>
      <td>SWEETWATER, WY</td>
    </tr>
    <tr>
      <th>3138</th>
      <td>56039</td>
      <td>21294</td>
      <td>3995.38</td>
      <td>5.3</td>
      <td>TETON, WY</td>
    </tr>
    <tr>
      <th>3139</th>
      <td>56041</td>
      <td>21118</td>
      <td>2081.26</td>
      <td>10.1</td>
      <td>UINTA, WY</td>
    </tr>
    <tr>
      <th>3140</th>
      <td>56043</td>
      <td>8533</td>
      <td>2238.55</td>
      <td>3.8</td>
      <td>WASHAKIE, WY</td>
    </tr>
    <tr>
      <th>3141</th>
      <td>56045</td>
      <td>7208</td>
      <td>2398.09</td>
      <td>3.0</td>
      <td>WESTON, WY</td>
    </tr>
  </tbody>
</table>
<p>3142 rows × 5 columns</p>
</div>




```python
df_counties.to_csv("counties.csv")
```


```python
db_connection_str = 'mysql+mysqlconnector://rl:trustno1@localhost/us_housing_prices_v2'
db_connection = create_engine(db_connection_str)

query = """
SELECT a.*
FROM `sales` a
INNER JOIN
(
    SELECT   `property_id`, `state`, `property_zip5`, `property_county`, MIN(`sale_datetime`) AS first_sale_datetime
    FROM     `sales`
    GROUP BY `property_id`
) b ON a.property_id = b.property_id AND a.sale_datetime = b.first_sale_datetime
WHERE b.first_sale_datetime > \"2009-06-30\" AND b.first_sale_datetime < \"2020-01-01\";
"""

raw_count_df = pd.read_sql(query, db_connection)
raw_count_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>state</th>
      <th>property_zip5</th>
      <th>property_street_address</th>
      <th>property_city</th>
      <th>property_county</th>
      <th>property_id</th>
      <th>sale_datetime</th>
      <th>property_type</th>
      <th>sale_price</th>
      <th>seller_1_name</th>
      <th>...</th>
      <th>land_assessed_date</th>
      <th>seller_1_state</th>
      <th>seller_2_state</th>
      <th>buyer_1_state</th>
      <th>buyer_2_state</th>
      <th>total_assessed_value</th>
      <th>total_appraised_value</th>
      <th>land_appraised_value</th>
      <th>building_appraised_value</th>
      <th>land_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AZ</td>
      <td>85140</td>
      <td>40162 N ORKNEY WAY</td>
      <td>SAN TAN VALLEY</td>
      <td>PINAL</td>
      <td>109235690</td>
      <td>2017-06-01</td>
      <td>RESIDENTIAL</td>
      <td>220000</td>
      <td>None</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AZ</td>
      <td>85140</td>
      <td>40164 N THOROUGHBRED WAY</td>
      <td>SAN TAN VALLEY</td>
      <td>PINAL</td>
      <td>9B37FF4B-FA81-43C5-A139-5DEBF5A0EE7C</td>
      <td>2018-10-01</td>
      <td>RESIDENTIAL</td>
      <td>205000</td>
      <td>None</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AZ</td>
      <td>85739</td>
      <td>40164 S RIDGELINE CT</td>
      <td>SADDLEBROOKE</td>
      <td>PINAL</td>
      <td>305931160</td>
      <td>2019-05-01</td>
      <td>RESIDENTIAL</td>
      <td>236000</td>
      <td>None</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AZ</td>
      <td>85138</td>
      <td>40164 W GANLY WAY</td>
      <td>MARICOPA</td>
      <td>PINAL</td>
      <td>512487380</td>
      <td>2017-03-01</td>
      <td>RESIDENTIAL</td>
      <td>186590</td>
      <td>None</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AZ</td>
      <td>85138</td>
      <td>40164 W HAYDEN DR</td>
      <td>MARICOPA</td>
      <td>PINAL</td>
      <td>512494720</td>
      <td>2019-08-05</td>
      <td>RESIDENTIAL</td>
      <td>0</td>
      <td>None</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>15473227</th>
      <td>WI</td>
      <td>None</td>
      <td>W5495 STATE ROAD 82</td>
      <td>LEMONWEIR</td>
      <td>JUNEAU</td>
      <td>PRCL290180387</td>
      <td>2019-03-12</td>
      <td>None</td>
      <td>0</td>
      <td>PETER JRULAND</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15473228</th>
      <td>WI</td>
      <td>None</td>
      <td>W54954 SCHULTZ ROAD</td>
      <td>ONALASKA</td>
      <td>LA CROSSE</td>
      <td>PRCL10-03078-000</td>
      <td>2019-06-11</td>
      <td>None</td>
      <td>650000</td>
      <td>KARL AND SHAUNASCHILLING</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15473229</th>
      <td>WI</td>
      <td>None</td>
      <td>W5496 ASPEN ROAD</td>
      <td>SPRINGWATER</td>
      <td>WAUSHARA</td>
      <td>PRCL032-03553-0200</td>
      <td>2017-04-24</td>
      <td>None</td>
      <td>215000</td>
      <td>LARRY J. AND DIANE F.PEROUTKA</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15473230</th>
      <td>WI</td>
      <td>None</td>
      <td>W5496 COUNTY ROAD X</td>
      <td>MIDDLE INLET</td>
      <td>MARINETTE</td>
      <td>PRCL018-00669.006</td>
      <td>2016-11-14</td>
      <td>None</td>
      <td>63500</td>
      <td>CLAIRE M. A/K/A CLAIRE A/K/A CLAIRE MARIEBOLANDER</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15473231</th>
      <td>WI</td>
      <td>None</td>
      <td>W5496 GROGAN ROAD</td>
      <td>WAUSAUKEE</td>
      <td>MARINETTE</td>
      <td>PRCL036-01346.003</td>
      <td>2016-05-06</td>
      <td>None</td>
      <td>0</td>
      <td>PEDER HHERREID</td>
      <td>...</td>
      <td>NaT</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>15473232 rows × 43 columns</p>
</div>



https://stackoverflow.com/questions/11683712/sql-group-by-and-min-mysql 


```python
raw_count_df.to_csv("~/raw.csv")
```


```python
result_df = pd.DataFrame(raw_count_df)
result_df = result_df[['state', 'property_county', 'sale_datetime', 'property_id']]
result_df = result_df.rename(columns={'property_county': 'county', 'state':'code'})
result_df['county'] = result_df['county'].apply(cleanup_county)
result_df['county_state'] = result_df['county'] + ', ' + result_df['code'] 
del result_df['county']
del result_df['code']
result_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sale_datetime</th>
      <th>property_id</th>
      <th>county_state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2017-06-01</td>
      <td>109235690</td>
      <td>PINAL, AZ</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>9B37FF4B-FA81-43C5-A139-5DEBF5A0EE7C</td>
      <td>PINAL, AZ</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2019-05-01</td>
      <td>305931160</td>
      <td>PINAL, AZ</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2017-03-01</td>
      <td>512487380</td>
      <td>PINAL, AZ</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2019-08-05</td>
      <td>512494720</td>
      <td>PINAL, AZ</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>15473227</th>
      <td>2019-03-12</td>
      <td>PRCL290180387</td>
      <td>JUNEAU, WI</td>
    </tr>
    <tr>
      <th>15473228</th>
      <td>2019-06-11</td>
      <td>PRCL10-03078-000</td>
      <td>LA CROSSE, WI</td>
    </tr>
    <tr>
      <th>15473229</th>
      <td>2017-04-24</td>
      <td>PRCL032-03553-0200</td>
      <td>WAUSHARA, WI</td>
    </tr>
    <tr>
      <th>15473230</th>
      <td>2016-11-14</td>
      <td>PRCL018-00669.006</td>
      <td>MARINETTE, WI</td>
    </tr>
    <tr>
      <th>15473231</th>
      <td>2016-05-06</td>
      <td>PRCL036-01346.003</td>
      <td>MARINETTE, WI</td>
    </tr>
  </tbody>
</table>
<p>15473232 rows × 3 columns</p>
</div>




```python
result_df.to_csv("~/result.csv")
```


```python
counts_by_county = result_df['county_state'].value_counts()
df_counts_by_county = pd.DataFrame.from_records([counts_by_county.to_dict()]).transpose()
df_counts_by_county.reset_index(inplace=True)
df_counts_by_county = df_counts_by_county.rename(columns={'index': 'county_state', 0: 'n'})
df_counts_by_county = df_counts_by_county.sort_values('n', ascending=False)

df_counts_by_county = pd.merge(df_counts_by_county, df_counties, on='county_state')

df_counts_by_county['per_capita'] = df_counts_by_county['n'] / df_counts_by_county['pop2010']
df_counts_by_county['per_area'] = df_counts_by_county['n'] / df_counts_by_county['area_2010']

per_capita_mean = float(df_counts_by_county['per_capita'].mean())
per_capita_stdev = float(df_counts_by_county['per_capita'].std())

df_counts_by_county['per_capita_stdevs_from_mean'] = (df_counts_by_county['per_capita'] - per_capita_mean) / per_capita_stdev

per_area_mean = float(df_counts_by_county['per_area'].mean())
per_area_stdev = float(df_counts_by_county['per_area'].std())

df_counts_by_county['per_area_stdevs_from_mean'] = (df_counts_by_county['per_area'] - per_area_mean) / per_area_stdev
df_counts_by_county = df_counts_by_county.sort_values('per_capita', ascending=False)

df_counts_by_county
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>county_state</th>
      <th>n</th>
      <th>fips</th>
      <th>pop2010</th>
      <th>area_2010</th>
      <th>density_2010</th>
      <th>per_capita</th>
      <th>per_area</th>
      <th>per_capita_stdevs_from_mean</th>
      <th>per_area_stdevs_from_mean</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>55</th>
      <td>CAPE MAY, NJ</td>
      <td>76871</td>
      <td>34009</td>
      <td>97265</td>
      <td>251.43</td>
      <td>386.9</td>
      <td>0.790325</td>
      <td>305.735195</td>
      <td>4.961530</td>
      <td>0.586059</td>
    </tr>
    <tr>
      <th>218</th>
      <td>GRAND, CO</td>
      <td>11284</td>
      <td>08049</td>
      <td>14843</td>
      <td>1846.33</td>
      <td>8.0</td>
      <td>0.760224</td>
      <td>6.111584</td>
      <td>4.709535</td>
      <td>-0.287329</td>
    </tr>
    <tr>
      <th>388</th>
      <td>HAMILTON, NY</td>
      <td>2860</td>
      <td>36041</td>
      <td>4836</td>
      <td>1717.37</td>
      <td>2.8</td>
      <td>0.591398</td>
      <td>1.665337</td>
      <td>3.296219</td>
      <td>-0.300290</td>
    </tr>
    <tr>
      <th>322</th>
      <td>CRAWFORD, IN</td>
      <td>5975</td>
      <td>18025</td>
      <td>10713</td>
      <td>305.64</td>
      <td>35.1</td>
      <td>0.557734</td>
      <td>19.549143</td>
      <td>3.014401</td>
      <td>-0.248159</td>
    </tr>
    <tr>
      <th>337</th>
      <td>SWITZERLAND, IN</td>
      <td>5365</td>
      <td>18155</td>
      <td>10613</td>
      <td>220.63</td>
      <td>48.1</td>
      <td>0.505512</td>
      <td>24.316729</td>
      <td>2.577232</td>
      <td>-0.234262</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>413</th>
      <td>EAGLE, CO</td>
      <td>51</td>
      <td>08037</td>
      <td>52197</td>
      <td>1684.53</td>
      <td>31.0</td>
      <td>0.000977</td>
      <td>0.030276</td>
      <td>-1.646455</td>
      <td>-0.305056</td>
    </tr>
    <tr>
      <th>409</th>
      <td>POLK, FL</td>
      <td>580</td>
      <td>12105</td>
      <td>602095</td>
      <td>1797.84</td>
      <td>334.9</td>
      <td>0.000963</td>
      <td>0.322609</td>
      <td>-1.646570</td>
      <td>-0.304204</td>
    </tr>
    <tr>
      <th>410</th>
      <td>INDIAN RIVER, FL</td>
      <td>91</td>
      <td>12061</td>
      <td>138028</td>
      <td>502.87</td>
      <td>274.5</td>
      <td>0.000659</td>
      <td>0.180961</td>
      <td>-1.649116</td>
      <td>-0.304616</td>
    </tr>
    <tr>
      <th>412</th>
      <td>HENDERSON, NC</td>
      <td>53</td>
      <td>37089</td>
      <td>106740</td>
      <td>373.07</td>
      <td>286.1</td>
      <td>0.000497</td>
      <td>0.142064</td>
      <td>-1.650478</td>
      <td>-0.304730</td>
    </tr>
    <tr>
      <th>414</th>
      <td>ARLINGTON, VA</td>
      <td>1</td>
      <td>51013</td>
      <td>207627</td>
      <td>25.97</td>
      <td>7993.6</td>
      <td>0.000005</td>
      <td>0.038506</td>
      <td>-1.654594</td>
      <td>-0.305032</td>
    </tr>
  </tbody>
</table>
<p>415 rows × 10 columns</p>
</div>




```python
df_counts_by_county.to_csv("~/counts.csv")
```

The above code computed a Pandas data frame with the following fields for each country:
* `n` - absolute number of new real estate projects (number of initial sale records).
* `per_capita` - `n` divided by population of the county (for year 2010).
* `per_area` - `n` divided by land area of the county (in square miles).
* `per_capita_stdevs_from_mean` - how many standard deviations is `per_capita` away from the mean value of entire data we're looking into
* `per_area_stdevs_from_mean` - equivalent of `per_capita` for `per_area` value.]

Based on the numbers that we have now, we can find the best and worst counties in terms of real estate development.

How does the overall distribution look like for per capita values and their distance from the mean? We can plot some simple bar charts to find out.


```python
import plotly.express as px
from IPython.display import Image

df_counts_by_county = df_counts_by_county.sort_values('per_capita', ascending=False)
fig = px.bar(df_counts_by_county, x='county_state', y='per_capita', title="New real estate sales per capita", labels=labels)
#fig.show()
fig.write_image("per_capita_by_county.png")
Image(filename="per_capita_by_county.png")
```




    
![png](output_16_0.png)
    




```python
fig = px.bar(df_counts_by_county, x='county_state', y='per_capita_stdevs_from_mean', title="New real estate sales per capita (stdevs. from mean)", labels=labels)
#fig.show()
fig.write_image("per_capita_stdevs_by_county.png")
Image(filename="per_capita_stdevs_by_county.png")
```




    
![png](output_17_0.png)
    



We can see that the spread is quite severe. Some counties have over two standard deviations more real estate developments more than the mean value (per capita). Some other counties lag behind by having their per capita number lower than one standard deviation below the mean. Let us look into some outliers on both extremes to try getting some ideas for explanation.

What are top 10 counties we know of with highest per capita number of new real estate projects?


```python
df_best_per_capita = df_counts_by_county.sort_values('per_capita', ascending=False).head(10)
df_best_per_capita
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>county_state</th>
      <th>n</th>
      <th>fips</th>
      <th>pop2010</th>
      <th>area_2010</th>
      <th>density_2010</th>
      <th>per_capita</th>
      <th>per_area</th>
      <th>per_capita_stdevs_from_mean</th>
      <th>per_area_stdevs_from_mean</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>55</th>
      <td>CAPE MAY, NJ</td>
      <td>76871</td>
      <td>34009</td>
      <td>97265</td>
      <td>251.43</td>
      <td>386.9</td>
      <td>0.790325</td>
      <td>305.735195</td>
      <td>4.961530</td>
      <td>0.586059</td>
    </tr>
    <tr>
      <th>218</th>
      <td>GRAND, CO</td>
      <td>11284</td>
      <td>08049</td>
      <td>14843</td>
      <td>1846.33</td>
      <td>8.0</td>
      <td>0.760224</td>
      <td>6.111584</td>
      <td>4.709535</td>
      <td>-0.287329</td>
    </tr>
    <tr>
      <th>388</th>
      <td>HAMILTON, NY</td>
      <td>2860</td>
      <td>36041</td>
      <td>4836</td>
      <td>1717.37</td>
      <td>2.8</td>
      <td>0.591398</td>
      <td>1.665337</td>
      <td>3.296219</td>
      <td>-0.300290</td>
    </tr>
    <tr>
      <th>322</th>
      <td>CRAWFORD, IN</td>
      <td>5975</td>
      <td>18025</td>
      <td>10713</td>
      <td>305.64</td>
      <td>35.1</td>
      <td>0.557734</td>
      <td>19.549143</td>
      <td>3.014401</td>
      <td>-0.248159</td>
    </tr>
    <tr>
      <th>337</th>
      <td>SWITZERLAND, IN</td>
      <td>5365</td>
      <td>18155</td>
      <td>10613</td>
      <td>220.63</td>
      <td>48.1</td>
      <td>0.505512</td>
      <td>24.316729</td>
      <td>2.577232</td>
      <td>-0.234262</td>
    </tr>
    <tr>
      <th>398</th>
      <td>OURAY, CO</td>
      <td>2223</td>
      <td>08091</td>
      <td>4436</td>
      <td>541.59</td>
      <td>8.2</td>
      <td>0.501127</td>
      <td>4.104581</td>
      <td>2.540523</td>
      <td>-0.293179</td>
    </tr>
    <tr>
      <th>290</th>
      <td>VERMILLION, IN</td>
      <td>7716</td>
      <td>18165</td>
      <td>16212</td>
      <td>256.88</td>
      <td>63.1</td>
      <td>0.475944</td>
      <td>30.037372</td>
      <td>2.329702</td>
      <td>-0.217587</td>
    </tr>
    <tr>
      <th>396</th>
      <td>GILPIN, CO</td>
      <td>2582</td>
      <td>08047</td>
      <td>5441</td>
      <td>149.90</td>
      <td>36.3</td>
      <td>0.474545</td>
      <td>17.224817</td>
      <td>2.317993</td>
      <td>-0.254934</td>
    </tr>
    <tr>
      <th>176</th>
      <td>STEUBEN, IN</td>
      <td>15832</td>
      <td>18151</td>
      <td>34185</td>
      <td>308.94</td>
      <td>110.7</td>
      <td>0.463127</td>
      <td>51.246197</td>
      <td>2.222408</td>
      <td>-0.155764</td>
    </tr>
    <tr>
      <th>203</th>
      <td>CHEROKEE, NC</td>
      <td>12386</td>
      <td>37039</td>
      <td>27444</td>
      <td>455.43</td>
      <td>60.3</td>
      <td>0.451319</td>
      <td>27.196276</td>
      <td>2.123558</td>
      <td>-0.225868</td>
    </tr>
  </tbody>
</table>
</div>




```python
labels = {
    "county_state": "County",
    "per_area": "new projects per square mile",
    "per_capita": "new projects per capita",
    "per_capita_stdevs_from_mean": "standard devs. from mean (per capita)",
    "per_area_stdevs_from_mean": "standard devs. from mean (per area)"
}

fig = px.bar(df_best_per_capita, x="county_state", y="per_capita", labels=labels)
#fig.show()
fig.write_image("top10_per_capita_by_county.png")
Image(filename="top10_per_capita_by_county.png")
```




    
![png](output_20_0.png)
    



* Cape May county, NJ lays on souther New Jersey and is quite attractive to tourists, which also makes it attractive to real estate investors. This county is somewhat urbanised by coastal beach towns and has a moderate population density - largest in the top 10 list, but significantly lower than that in San Francisco.
* Grand County, CO is mostly rural place with low population density. The same applies to all entries on top 10 list except Cape May, NJ. It's probably not significant that these locations have high per capita amount of real estate developments, given that populations are small and per area amounts of new projects are below average. Unsurprisingly, lack of population means lack of NIMBYs.

What are the 10 counties with lowest per capita amounts of new real estate projects?


```python
df_worst_per_capita = df_counts_by_county.sort_values('per_capita', ascending=True).head(10)
df_worst_per_capita
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>county_state</th>
      <th>n</th>
      <th>fips</th>
      <th>pop2010</th>
      <th>area_2010</th>
      <th>density_2010</th>
      <th>per_capita</th>
      <th>per_area</th>
      <th>per_capita_stdevs_from_mean</th>
      <th>per_area_stdevs_from_mean</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>414</th>
      <td>ARLINGTON, VA</td>
      <td>1</td>
      <td>51013</td>
      <td>207627</td>
      <td>25.97</td>
      <td>7993.6</td>
      <td>0.000005</td>
      <td>0.038506</td>
      <td>-1.654594</td>
      <td>-0.305032</td>
    </tr>
    <tr>
      <th>412</th>
      <td>HENDERSON, NC</td>
      <td>53</td>
      <td>37089</td>
      <td>106740</td>
      <td>373.07</td>
      <td>286.1</td>
      <td>0.000497</td>
      <td>0.142064</td>
      <td>-1.650478</td>
      <td>-0.304730</td>
    </tr>
    <tr>
      <th>410</th>
      <td>INDIAN RIVER, FL</td>
      <td>91</td>
      <td>12061</td>
      <td>138028</td>
      <td>502.87</td>
      <td>274.5</td>
      <td>0.000659</td>
      <td>0.180961</td>
      <td>-1.649116</td>
      <td>-0.304616</td>
    </tr>
    <tr>
      <th>409</th>
      <td>POLK, FL</td>
      <td>580</td>
      <td>12105</td>
      <td>602095</td>
      <td>1797.84</td>
      <td>334.9</td>
      <td>0.000963</td>
      <td>0.322609</td>
      <td>-1.646570</td>
      <td>-0.304204</td>
    </tr>
    <tr>
      <th>413</th>
      <td>EAGLE, CO</td>
      <td>51</td>
      <td>08037</td>
      <td>52197</td>
      <td>1684.53</td>
      <td>31.0</td>
      <td>0.000977</td>
      <td>0.030276</td>
      <td>-1.646455</td>
      <td>-0.305056</td>
    </tr>
    <tr>
      <th>411</th>
      <td>JEFFERSON, WA</td>
      <td>77</td>
      <td>53031</td>
      <td>29872</td>
      <td>1803.70</td>
      <td>16.6</td>
      <td>0.002578</td>
      <td>0.042690</td>
      <td>-1.633056</td>
      <td>-0.305020</td>
    </tr>
    <tr>
      <th>358</th>
      <td>HARTFORD, CT</td>
      <td>4367</td>
      <td>09003</td>
      <td>894014</td>
      <td>735.10</td>
      <td>1216.2</td>
      <td>0.004885</td>
      <td>5.940688</td>
      <td>-1.613743</td>
      <td>-0.287827</td>
    </tr>
    <tr>
      <th>165</th>
      <td>RIVERSIDE, CA</td>
      <td>17841</td>
      <td>06065</td>
      <td>2189641</td>
      <td>7206.48</td>
      <td>303.8</td>
      <td>0.008148</td>
      <td>2.475689</td>
      <td>-1.586425</td>
      <td>-0.297927</td>
    </tr>
    <tr>
      <th>393</th>
      <td>CLAYTON, GA</td>
      <td>2606</td>
      <td>13063</td>
      <td>259424</td>
      <td>141.57</td>
      <td>1832.5</td>
      <td>0.010045</td>
      <td>18.407855</td>
      <td>-1.570541</td>
      <td>-0.251486</td>
    </tr>
    <tr>
      <th>210</th>
      <td>FAIRFAX, VA</td>
      <td>11802</td>
      <td>51059</td>
      <td>1081726</td>
      <td>390.97</td>
      <td>2766.8</td>
      <td>0.010910</td>
      <td>30.186459</td>
      <td>-1.563299</td>
      <td>-0.217152</td>
    </tr>
  </tbody>
</table>
</div>




```python
fig = px.bar(df_worst_per_capita, x="county_state", y="per_capita", labels=labels)
#fig.show()
fig.write_image("worst10_per_capita_by_county.png")
Image(filename="worst10_per_capita_by_county.png")
```




    
![png](output_23_0.png)
    



* Arlington, VA had only one sales record scraped during the bounty, so it's safe to say this is statistical anomaly.
* Most (but not all) of these areas are more urbanised with population density exceeding 200 people per square mile.
* Indian river county is on the eastern coast of Florida that is home to some fairly wealthy beach towns. 
* Polk county, FL is fairly populous urban area with central Florida with know resident opposition to new real estate projects.
* Riverside, CA is known to be difficult for real estate developers due to [zoning and resident opposition](https://iebizjournal.com/local-land-use-decisions-nimbyism-are-leading-causes-behind-southern-californias-lack-of-housing-production-across-price-levels/).
* Clayton county, GA overlaps with Atlanta metropolitan area that is known for [NIMBY activity](https://www.popeandland.com/atlanta-suburbs-grapple-with-nimbys-and-housing-affordability/).

Now, let us look into per-area numbers using the same kind of charts.


```python
df_counts_by_county = df_counts_by_county.sort_values('per_area', ascending=False)
fig = px.bar(df_counts_by_county, x='county_state', y='per_area', title="New real estate sales per area", labels=labels)
#fig.show()
fig.write_image("per_area_by_county.png")
Image(filename="per_area_by_county.png")
```




    
![png](output_25_0.png)
    




```python
df_counts_by_county = df_counts_by_county.sort_values('per_area_stdevs_from_mean', ascending=False)
fig = px.bar(df_counts_by_county, x='county_state', y='per_area_stdevs_from_mean', title="New real estate sales per area (stdevs. from mean)", labels=labels)
#fig.show()
fig.write_image("per_area_stdevs_by_county.png")
Image(filename="per_area_stdevs_by_county.png")
```




    
![png](output_26_0.png)
    




```python
df_best_per_area = df_counts_by_county.sort_values('per_area', ascending=False).head(10)
df_best_per_area
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>county_state</th>
      <th>n</th>
      <th>fips</th>
      <th>pop2010</th>
      <th>area_2010</th>
      <th>density_2010</th>
      <th>per_capita</th>
      <th>per_area</th>
      <th>per_capita_stdevs_from_mean</th>
      <th>per_area_stdevs_from_mean</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>32</th>
      <td>NEW YORK, NY</td>
      <td>112059</td>
      <td>36061</td>
      <td>1585873</td>
      <td>22.83</td>
      <td>69467.5</td>
      <td>0.070661</td>
      <td>4908.409987</td>
      <td>-1.063102</td>
      <td>14.002622</td>
    </tr>
    <tr>
      <th>28</th>
      <td>HUDSON, NJ</td>
      <td>118243</td>
      <td>34017</td>
      <td>634266</td>
      <td>46.19</td>
      <td>13731.4</td>
      <td>0.186425</td>
      <td>2559.926391</td>
      <td>-0.093989</td>
      <td>7.156912</td>
    </tr>
    <tr>
      <th>5</th>
      <td>PHILADELPHIA, PA</td>
      <td>301665</td>
      <td>42101</td>
      <td>1526006</td>
      <td>134.10</td>
      <td>11379.5</td>
      <td>0.197683</td>
      <td>2249.552573</td>
      <td>0.000255</td>
      <td>6.252188</td>
    </tr>
    <tr>
      <th>18</th>
      <td>KINGS, NY</td>
      <td>141742</td>
      <td>36047</td>
      <td>2504700</td>
      <td>70.82</td>
      <td>35369.1</td>
      <td>0.056590</td>
      <td>2001.440271</td>
      <td>-1.180891</td>
      <td>5.528953</td>
    </tr>
    <tr>
      <th>35</th>
      <td>SUFFOLK, MA</td>
      <td>100841</td>
      <td>25025</td>
      <td>722023</td>
      <td>58.15</td>
      <td>12415.7</td>
      <td>0.139665</td>
      <td>1734.153052</td>
      <td>-0.485441</td>
      <td>4.749824</td>
    </tr>
    <tr>
      <th>23</th>
      <td>QUEENS, NY</td>
      <td>131720</td>
      <td>36081</td>
      <td>2230722</td>
      <td>108.53</td>
      <td>20553.6</td>
      <td>0.059048</td>
      <td>1213.673639</td>
      <td>-1.160316</td>
      <td>3.232653</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ESSEX, NJ</td>
      <td>135494</td>
      <td>34013</td>
      <td>783969</td>
      <td>126.21</td>
      <td>6211.5</td>
      <td>0.172831</td>
      <td>1073.559940</td>
      <td>-0.207791</td>
      <td>2.824229</td>
    </tr>
    <tr>
      <th>34</th>
      <td>UNION, NJ</td>
      <td>104862</td>
      <td>34039</td>
      <td>536499</td>
      <td>102.86</td>
      <td>5216.1</td>
      <td>0.195456</td>
      <td>1019.463348</td>
      <td>-0.018385</td>
      <td>2.666540</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BERGEN, NJ</td>
      <td>196642</td>
      <td>34003</td>
      <td>905116</td>
      <td>233.01</td>
      <td>3884.5</td>
      <td>0.217256</td>
      <td>843.920862</td>
      <td>0.164113</td>
      <td>2.154843</td>
    </tr>
    <tr>
      <th>4</th>
      <td>MARION, IN</td>
      <td>320546</td>
      <td>18097</td>
      <td>903393</td>
      <td>396.30</td>
      <td>2279.6</td>
      <td>0.354825</td>
      <td>808.846833</td>
      <td>1.315759</td>
      <td>2.052604</td>
    </tr>
  </tbody>
</table>
</div>




```python
fig = px.bar(df_best_per_area, x="county_state", y="per_area", labels=labels)
#fig.show()
fig.write_image("top10_per_area_by_count.png")
Image(filename="top10_per_area_by_count.png")
```




    
![png](output_28_0.png)
    



* A strong outlier here is New York, which is probably unsuprising.
* Hudson, NJ is the most densely populated county in New Jersey. Some other counties of NJ are also represented.
* Philadelpia, PA is (was?) undergoing real estate boom that included some billion-dollar projects.
* Suffolk county, MA is building some 10 000 new housing units.

We believe that data analysis should be reproducible. Reproducibility of data science requires two pillars: open data and free, open software. In this particular case, open data requirement is fullfilled by `us-housing-prices-v2` database, hosted on DoltHub. The latter requirement is fullfilled by Dolt DMBS and a Pythonic data science environment based on Jupyter. We did the heavy lifting of tabular data with Pandas library. To establish connection between Dolt running in server mode and Pandas we used MySQL connector Python module with SQLAlchemy (Dolt is compatible-enough with MySQL for this to work). Plotly was used to visualise county-level data on interactive map view. Some supporting geospatial data was downloaded with Python `requests` module.
