```python
from io import StringIO

import pandas as pd
import requests
import mysql.connector as connection
from sqlalchemy import create_engine
```


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
        
    if fips.startswith("<a") and fips.endswith("</a>"):
        fips = fips.split(">")[1].split("<")[0]
        
    if len(fips) == 4:
        fips = '0' + fips
    
    return fips
    
resp = requests.get("https://www.openintro.org/data/csv/county_complete.csv", 
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"})

buf = StringIO(resp.text)

df_counties = pd.read_csv(buf, dtype={'fips': str})
df_counties = df_counties[["fips", "state", "name", "area_2010"]]
df_counties = df_counties.rename(columns={'name': 'county'})
df_counties['county'] = df_counties['county'].apply(cleanup_county)
df_counties['fips'] = df_counties['fips'].apply(fix_fips)
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
      <th>state</th>
      <th>county</th>
      <th>area_2010</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>01001</td>
      <td>Alabama</td>
      <td>AUTAUGA</td>
      <td>594.44</td>
    </tr>
    <tr>
      <th>1</th>
      <td>01003</td>
      <td>Alabama</td>
      <td>BALDWIN</td>
      <td>1589.78</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01005</td>
      <td>Alabama</td>
      <td>BARBOUR</td>
      <td>884.88</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01007</td>
      <td>Alabama</td>
      <td>BIBB</td>
      <td>622.58</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01009</td>
      <td>Alabama</td>
      <td>BLOUNT</td>
      <td>644.78</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3137</th>
      <td>56037</td>
      <td>Wyoming</td>
      <td>SWEETWATER</td>
      <td>10426.65</td>
    </tr>
    <tr>
      <th>3138</th>
      <td>56039</td>
      <td>Wyoming</td>
      <td>TETON</td>
      <td>3995.38</td>
    </tr>
    <tr>
      <th>3139</th>
      <td>56041</td>
      <td>Wyoming</td>
      <td>UINTA</td>
      <td>2081.26</td>
    </tr>
    <tr>
      <th>3140</th>
      <td>56043</td>
      <td>Wyoming</td>
      <td>WASHAKIE</td>
      <td>2238.55</td>
    </tr>
    <tr>
      <th>3141</th>
      <td>56045</td>
      <td>Wyoming</td>
      <td>WESTON</td>
      <td>2398.09</td>
    </tr>
  </tbody>
</table>
<p>3142 rows × 4 columns</p>
</div>




```python
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
      <th>area_2010</th>
      <th>county_state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>01001</td>
      <td>594.44</td>
      <td>AUTAUGA, AL</td>
    </tr>
    <tr>
      <th>1</th>
      <td>01003</td>
      <td>1589.78</td>
      <td>BALDWIN, AL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01005</td>
      <td>884.88</td>
      <td>BARBOUR, AL</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01007</td>
      <td>622.58</td>
      <td>BIBB, AL</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01009</td>
      <td>644.78</td>
      <td>BLOUNT, AL</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3137</th>
      <td>56037</td>
      <td>10426.65</td>
      <td>SWEETWATER, WY</td>
    </tr>
    <tr>
      <th>3138</th>
      <td>56039</td>
      <td>3995.38</td>
      <td>TETON, WY</td>
    </tr>
    <tr>
      <th>3139</th>
      <td>56041</td>
      <td>2081.26</td>
      <td>UINTA, WY</td>
    </tr>
    <tr>
      <th>3140</th>
      <td>56043</td>
      <td>2238.55</td>
      <td>WASHAKIE, WY</td>
    </tr>
    <tr>
      <th>3141</th>
      <td>56045</td>
      <td>2398.09</td>
      <td>WESTON, WY</td>
    </tr>
  </tbody>
</table>
<p>3142 rows × 3 columns</p>
</div>




```python
df_proximity = pd.read_csv("http://proximityone.com/countytrends/cb_2015_us_county_500k_bp_1415_annual_table.csv",
                          dtype={'GEOID': str})
df_proximity
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
      <th>NAMELSAD</th>
      <th>STUSPS</th>
      <th>LSAD</th>
      <th>GEOID</th>
      <th>CBSAFP</th>
      <th>POP2014</th>
      <th>POP2015</th>
      <th>HU2014</th>
      <th>HU2015</th>
      <th>BLDG_14</th>
      <th>...</th>
      <th>V1_15</th>
      <th>B2_15</th>
      <th>U2_15</th>
      <th>V2_15</th>
      <th>B34_15</th>
      <th>U34_15</th>
      <th>V34_15</th>
      <th>B5_15</th>
      <th>U5_15</th>
      <th>V5_15</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Autauga County</td>
      <td>AL</td>
      <td>1</td>
      <td>01001</td>
      <td>33860.0</td>
      <td>55290</td>
      <td>55347</td>
      <td>22751</td>
      <td>22847</td>
      <td>131</td>
      <td>...</td>
      <td>39749354.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Baldwin County</td>
      <td>AL</td>
      <td>1</td>
      <td>&lt;a href=http://proximityone.com/rdems/1/rdems0...</td>
      <td>19300.0</td>
      <td>199713</td>
      <td>203709</td>
      <td>107368</td>
      <td>108564</td>
      <td>1373</td>
      <td>...</td>
      <td>302576607.0</td>
      <td>11</td>
      <td>22</td>
      <td>2232258.0</td>
      <td>29</td>
      <td>109</td>
      <td>12724884.0</td>
      <td>31</td>
      <td>450</td>
      <td>43856188.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Barbour County</td>
      <td>AL</td>
      <td>1</td>
      <td>01005</td>
      <td>NaN</td>
      <td>26815</td>
      <td>26489</td>
      <td>11799</td>
      <td>11789</td>
      <td>7</td>
      <td>...</td>
      <td>3292300.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Bibb County</td>
      <td>AL</td>
      <td>1</td>
      <td>01007</td>
      <td>13820.0</td>
      <td>22549</td>
      <td>22583</td>
      <td>8977</td>
      <td>8986</td>
      <td>19</td>
      <td>...</td>
      <td>2222180.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Blount County</td>
      <td>AL</td>
      <td>1</td>
      <td>01009</td>
      <td>13820.0</td>
      <td>57658</td>
      <td>57673</td>
      <td>23826</td>
      <td>23817</td>
      <td>3</td>
      <td>...</td>
      <td>1573173.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>2</td>
      <td>40</td>
      <td>3831302.0</td>
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
      <th>3137</th>
      <td>Sweetwater County</td>
      <td>WY</td>
      <td>56</td>
      <td>56037</td>
      <td>40540.0</td>
      <td>44925</td>
      <td>44626</td>
      <td>19077</td>
      <td>19245</td>
      <td>120</td>
      <td>...</td>
      <td>20852748.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3138</th>
      <td>Teton County</td>
      <td>WY</td>
      <td>56</td>
      <td>56039</td>
      <td>27220.0</td>
      <td>22905</td>
      <td>23125</td>
      <td>13269</td>
      <td>13395</td>
      <td>137</td>
      <td>...</td>
      <td>234204021.0</td>
      <td>10</td>
      <td>20</td>
      <td>6177535.0</td>
      <td>8</td>
      <td>30</td>
      <td>1164993.0</td>
      <td>1</td>
      <td>12</td>
      <td>1296756.0</td>
    </tr>
    <tr>
      <th>3139</th>
      <td>Uinta County</td>
      <td>WY</td>
      <td>56</td>
      <td>56041</td>
      <td>21740.0</td>
      <td>20903</td>
      <td>20822</td>
      <td>8773</td>
      <td>8788</td>
      <td>35</td>
      <td>...</td>
      <td>7856953.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3140</th>
      <td>Washakie County</td>
      <td>WY</td>
      <td>56</td>
      <td>56043</td>
      <td>NaN</td>
      <td>8316</td>
      <td>8328</td>
      <td>3810</td>
      <td>3804</td>
      <td>4</td>
      <td>...</td>
      <td>970000.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3141</th>
      <td>Weston County</td>
      <td>WY</td>
      <td>56</td>
      <td>56045</td>
      <td>NaN</td>
      <td>7185</td>
      <td>7234</td>
      <td>3500</td>
      <td>3489</td>
      <td>1</td>
      <td>...</td>
      <td>277200.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
<p>3142 rows × 42 columns</p>
</div>




```python
df_proximity = df_proximity.rename(columns={
    'NAMELSAD': 'county',
    'STUSPS': 'code',
    'GEOID': 'fips'
})
df_xref = pd.DataFrame(df_proximity)
df_xref['county'] = df_xref['county'].apply(cleanup_county)
df_xref['county_state'] = df_xref['county'] + ', ' + df_xref['code']
df_xref['population'] = (df_xref['POP2014'] + df_xref['POP2015']) / 2
df_xref['housing'] = (df_xref['HU2014'] + df_xref['HU2015']) / 2
df_xref['per_capita'] = df_proximity['housing'] / df_proximity['population']
df_xref = df_xref[["county_state", "population", "housing", "per_capita"]]
df_xref
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
      <th>population</th>
      <th>housing</th>
      <th>per_capita</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AUTAUGA, AL</td>
      <td>55318.5</td>
      <td>22799.0</td>
      <td>0.412141</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BALDWIN, AL</td>
      <td>201711.0</td>
      <td>107966.0</td>
      <td>0.535251</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BARBOUR, AL</td>
      <td>26652.0</td>
      <td>11794.0</td>
      <td>0.442518</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BIBB, AL</td>
      <td>22566.0</td>
      <td>8981.5</td>
      <td>0.398010</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BLOUNT, AL</td>
      <td>57665.5</td>
      <td>23821.5</td>
      <td>0.413098</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3137</th>
      <td>SWEETWATER, WY</td>
      <td>44775.5</td>
      <td>19161.0</td>
      <td>0.427935</td>
    </tr>
    <tr>
      <th>3138</th>
      <td>TETON, WY</td>
      <td>23015.0</td>
      <td>13332.0</td>
      <td>0.579274</td>
    </tr>
    <tr>
      <th>3139</th>
      <td>UINTA, WY</td>
      <td>20862.5</td>
      <td>8780.5</td>
      <td>0.420875</td>
    </tr>
    <tr>
      <th>3140</th>
      <td>WASHAKIE, WY</td>
      <td>8322.0</td>
      <td>3807.0</td>
      <td>0.457462</td>
    </tr>
    <tr>
      <th>3141</th>
      <td>WESTON, WY</td>
      <td>7209.5</td>
      <td>3494.5</td>
      <td>0.484708</td>
    </tr>
  </tbody>
</table>
<p>3142 rows × 4 columns</p>
</div>




```python
df_xref = pd.merge(df_counties, df_xref, on='county_state')
df_xref['per_area'] = df_xref['housing'] / df_xref['area_2010']
df_xref
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
      <th>area_2010</th>
      <th>county_state</th>
      <th>population</th>
      <th>housing</th>
      <th>per_capita</th>
      <th>per_area</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>01001</td>
      <td>594.44</td>
      <td>AUTAUGA, AL</td>
      <td>55318.5</td>
      <td>22799.0</td>
      <td>0.412141</td>
      <td>38.353745</td>
    </tr>
    <tr>
      <th>1</th>
      <td>01003</td>
      <td>1589.78</td>
      <td>BALDWIN, AL</td>
      <td>201711.0</td>
      <td>107966.0</td>
      <td>0.535251</td>
      <td>67.912541</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01005</td>
      <td>884.88</td>
      <td>BARBOUR, AL</td>
      <td>26652.0</td>
      <td>11794.0</td>
      <td>0.442518</td>
      <td>13.328361</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01007</td>
      <td>622.58</td>
      <td>BIBB, AL</td>
      <td>22566.0</td>
      <td>8981.5</td>
      <td>0.398010</td>
      <td>14.426258</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01009</td>
      <td>644.78</td>
      <td>BLOUNT, AL</td>
      <td>57665.5</td>
      <td>23821.5</td>
      <td>0.413098</td>
      <td>36.945160</td>
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
    </tr>
    <tr>
      <th>3141</th>
      <td>56037</td>
      <td>10426.65</td>
      <td>SWEETWATER, WY</td>
      <td>44775.5</td>
      <td>19161.0</td>
      <td>0.427935</td>
      <td>1.837695</td>
    </tr>
    <tr>
      <th>3142</th>
      <td>56039</td>
      <td>3995.38</td>
      <td>TETON, WY</td>
      <td>23015.0</td>
      <td>13332.0</td>
      <td>0.579274</td>
      <td>3.336854</td>
    </tr>
    <tr>
      <th>3143</th>
      <td>56041</td>
      <td>2081.26</td>
      <td>UINTA, WY</td>
      <td>20862.5</td>
      <td>8780.5</td>
      <td>0.420875</td>
      <td>4.218839</td>
    </tr>
    <tr>
      <th>3144</th>
      <td>56043</td>
      <td>2238.55</td>
      <td>WASHAKIE, WY</td>
      <td>8322.0</td>
      <td>3807.0</td>
      <td>0.457462</td>
      <td>1.700654</td>
    </tr>
    <tr>
      <th>3145</th>
      <td>56045</td>
      <td>2398.09</td>
      <td>WESTON, WY</td>
      <td>7209.5</td>
      <td>3494.5</td>
      <td>0.484708</td>
      <td>1.457201</td>
    </tr>
  </tbody>
</table>
<p>3146 rows × 7 columns</p>
</div>



## NOTE: the following requires the CSV file from the main notebook


```python
df_counts_by_county = pd.read_csv("~/counts.csv")
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
      <th>Unnamed: 0</th>
      <th>county_state</th>
      <th>n</th>
      <th>fips</th>
      <th>population</th>
      <th>area_2010</th>
      <th>density</th>
      <th>per_capita</th>
      <th>per_area</th>
      <th>per_capita_stdevs_from_mean</th>
      <th>per_area_stdevs_from_mean</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>55</td>
      <td>CAPE MAY, NJ</td>
      <td>76871</td>
      <td>34009</td>
      <td>94680.1</td>
      <td>251.43</td>
      <td>376.566440</td>
      <td>0.811902</td>
      <td>305.735195</td>
      <td>5.137887</td>
      <td>0.586059</td>
    </tr>
    <tr>
      <th>1</th>
      <td>218</td>
      <td>GRAND, CO</td>
      <td>11284</td>
      <td>8049</td>
      <td>14910.4</td>
      <td>1846.33</td>
      <td>8.075696</td>
      <td>0.756787</td>
      <td>6.111584</td>
      <td>4.678022</td>
      <td>-0.287329</td>
    </tr>
    <tr>
      <th>2</th>
      <td>388</td>
      <td>HAMILTON, NY</td>
      <td>2860</td>
      <td>36041</td>
      <td>4657.5</td>
      <td>1717.37</td>
      <td>2.711996</td>
      <td>0.614063</td>
      <td>1.665337</td>
      <td>3.487175</td>
      <td>-0.300290</td>
    </tr>
    <tr>
      <th>3</th>
      <td>322</td>
      <td>CRAWFORD, IN</td>
      <td>5975</td>
      <td>18025</td>
      <td>10612.7</td>
      <td>305.64</td>
      <td>34.722877</td>
      <td>0.563005</td>
      <td>19.549143</td>
      <td>3.061156</td>
      <td>-0.248159</td>
    </tr>
    <tr>
      <th>4</th>
      <td>337</td>
      <td>SWITZERLAND, IN</td>
      <td>5365</td>
      <td>18155</td>
      <td>10650.3</td>
      <td>220.63</td>
      <td>48.272220</td>
      <td>0.503742</td>
      <td>24.316729</td>
      <td>2.566683</td>
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
      <td>...</td>
    </tr>
    <tr>
      <th>410</th>
      <td>413</td>
      <td>EAGLE, CO</td>
      <td>51</td>
      <td>8037</td>
      <td>53629.9</td>
      <td>1684.53</td>
      <td>31.836714</td>
      <td>0.000951</td>
      <td>0.030276</td>
      <td>-1.628458</td>
      <td>-0.305056</td>
    </tr>
    <tr>
      <th>411</th>
      <td>409</td>
      <td>POLK, FL</td>
      <td>580</td>
      <td>12105</td>
      <td>651910.4</td>
      <td>1797.84</td>
      <td>362.607574</td>
      <td>0.000890</td>
      <td>0.322609</td>
      <td>-1.628969</td>
      <td>-0.304204</td>
    </tr>
    <tr>
      <th>412</th>
      <td>410</td>
      <td>INDIAN RIVER, FL</td>
      <td>91</td>
      <td>12061</td>
      <td>147425.7</td>
      <td>502.87</td>
      <td>293.168612</td>
      <td>0.000617</td>
      <td>0.180961</td>
      <td>-1.631242</td>
      <td>-0.304616</td>
    </tr>
    <tr>
      <th>413</th>
      <td>412</td>
      <td>HENDERSON, NC</td>
      <td>53</td>
      <td>37089</td>
      <td>111636.5</td>
      <td>373.07</td>
      <td>299.237409</td>
      <td>0.000475</td>
      <td>0.142064</td>
      <td>-1.632431</td>
      <td>-0.304730</td>
    </tr>
    <tr>
      <th>414</th>
      <td>414</td>
      <td>ARLINGTON, VA</td>
      <td>1</td>
      <td>51013</td>
      <td>226934.4</td>
      <td>25.97</td>
      <td>8738.328841</td>
      <td>0.000004</td>
      <td>0.038506</td>
      <td>-1.636356</td>
      <td>-0.305032</td>
    </tr>
  </tbody>
</table>
<p>415 rows × 11 columns</p>
</div>




```python
df_xref = pd.merge(df_xref, df_counts_by_county, on='county_state')
df_xref = df_xref[["county_state", "per_capita_x", "per_capita_y", "per_area_x", "per_area_y"]]
df_xref
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
      <th>per_capita_x</th>
      <th>per_capita_y</th>
      <th>per_area_x</th>
      <th>per_area_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PINAL, AZ</td>
      <td>0.412744</td>
      <td>0.226846</td>
      <td>30.889032</td>
      <td>17.264579</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BUTTE, CA</td>
      <td>0.435035</td>
      <td>0.044836</td>
      <td>59.739926</td>
      <td>6.122362</td>
    </tr>
    <tr>
      <th>2</th>
      <td>LOS ANGELES, CA</td>
      <td>0.344813</td>
      <td>0.021458</td>
      <td>861.621709</td>
      <td>52.872682</td>
    </tr>
    <tr>
      <th>3</th>
      <td>RIVERSIDE, CA</td>
      <td>0.351140</td>
      <td>0.007643</td>
      <td>114.245859</td>
      <td>2.475689</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SAN FRANCISCO, CA</td>
      <td>0.452894</td>
      <td>0.040715</td>
      <td>8297.194367</td>
      <td>739.364199</td>
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
      <th>412</th>
      <td>WAUKESHA, WI</td>
      <td>0.411668</td>
      <td>0.105881</td>
      <td>296.566406</td>
      <td>76.428844</td>
    </tr>
    <tr>
      <th>413</th>
      <td>WAUPACA, WI</td>
      <td>0.489614</td>
      <td>0.163931</td>
      <td>34.061334</td>
      <td>11.326584</td>
    </tr>
    <tr>
      <th>414</th>
      <td>WAUSHARA, WI</td>
      <td>0.618284</td>
      <td>0.231045</td>
      <td>23.800208</td>
      <td>8.959514</td>
    </tr>
    <tr>
      <th>415</th>
      <td>WINNEBAGO, WI</td>
      <td>0.438507</td>
      <td>0.112160</td>
      <td>171.160441</td>
      <td>43.743239</td>
    </tr>
    <tr>
      <th>416</th>
      <td>WOOD, WI</td>
      <td>0.469034</td>
      <td>0.119390</td>
      <td>43.475767</td>
      <td>11.087856</td>
    </tr>
  </tbody>
</table>
<p>417 rows × 5 columns</p>
</div>




```python
df_xref.to_csv("~/xref.csv")
```


```python
df_xref.sort_values('per_capita_y', ascending=False).head(10)
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
      <th>per_capita_x</th>
      <th>per_capita_y</th>
      <th>per_area_x</th>
      <th>per_area_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>199</th>
      <td>CAPE MAY, NJ</td>
      <td>1.041181</td>
      <td>0.811902</td>
      <td>393.576741</td>
      <td>305.735195</td>
    </tr>
    <tr>
      <th>14</th>
      <td>GRAND, CO</td>
      <td>1.118933</td>
      <td>0.756787</td>
      <td>8.815326</td>
      <td>6.111584</td>
    </tr>
    <tr>
      <th>236</th>
      <td>HAMILTON, NY</td>
      <td>1.861754</td>
      <td>0.614063</td>
      <td>5.104899</td>
      <td>1.665337</td>
    </tr>
    <tr>
      <th>66</th>
      <td>CRAWFORD, IN</td>
      <td>0.516248</td>
      <td>0.563005</td>
      <td>17.828164</td>
      <td>19.549143</td>
    </tr>
    <tr>
      <th>130</th>
      <td>SWITZERLAND, IN</td>
      <td>0.485533</td>
      <td>0.503742</td>
      <td>23.122422</td>
      <td>24.316729</td>
    </tr>
    <tr>
      <th>135</th>
      <td>VERMILLION, IN</td>
      <td>0.475977</td>
      <td>0.490465</td>
      <td>29.077780</td>
      <td>30.037372</td>
    </tr>
    <tr>
      <th>20</th>
      <td>OURAY, CO</td>
      <td>0.676793</td>
      <td>0.477264</td>
      <td>5.820824</td>
      <td>4.104581</td>
    </tr>
    <tr>
      <th>128</th>
      <td>STEUBEN, IN</td>
      <td>0.568467</td>
      <td>0.460429</td>
      <td>63.277659</td>
      <td>51.246197</td>
    </tr>
    <tr>
      <th>283</th>
      <td>CHEROKEE, NC</td>
      <td>0.652778</td>
      <td>0.448744</td>
      <td>38.889621</td>
      <td>27.196276</td>
    </tr>
    <tr>
      <th>13</th>
      <td>GILPIN, CO</td>
      <td>0.620779</td>
      <td>0.447169</td>
      <td>23.915944</td>
      <td>17.224817</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_xref.sort_values('per_capita_y', ascending=True).head(10)
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
      <th>per_capita_x</th>
      <th>per_capita_y</th>
      <th>per_area_x</th>
      <th>per_area_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>336</th>
      <td>ARLINGTON, VA</td>
      <td>0.489526</td>
      <td>0.000004</td>
      <td>4295.725838</td>
      <td>0.038506</td>
    </tr>
    <tr>
      <th>291</th>
      <td>HENDERSON, NC</td>
      <td>0.497066</td>
      <td>0.000475</td>
      <td>148.954620</td>
      <td>0.142064</td>
    </tr>
    <tr>
      <th>34</th>
      <td>INDIAN RIVER, FL</td>
      <td>0.530378</td>
      <td>0.000617</td>
      <td>154.351025</td>
      <td>0.180961</td>
    </tr>
    <tr>
      <th>45</th>
      <td>POLK, FL</td>
      <td>0.441172</td>
      <td>0.000890</td>
      <td>157.706748</td>
      <td>0.322609</td>
    </tr>
    <tr>
      <th>11</th>
      <td>EAGLE, CO</td>
      <td>0.592989</td>
      <td>0.000951</td>
      <td>18.768143</td>
      <td>0.030276</td>
    </tr>
    <tr>
      <th>341</th>
      <td>JEFFERSON, WA</td>
      <td>0.592827</td>
      <td>0.002512</td>
      <td>9.970616</td>
      <td>0.042690</td>
    </tr>
    <tr>
      <th>27</th>
      <td>HARTFORD, CT</td>
      <td>0.418149</td>
      <td>0.004877</td>
      <td>509.876887</td>
      <td>5.940688</td>
    </tr>
    <tr>
      <th>3</th>
      <td>RIVERSIDE, CA</td>
      <td>0.351140</td>
      <td>0.007643</td>
      <td>114.245859</td>
      <td>2.475689</td>
    </tr>
    <tr>
      <th>49</th>
      <td>CLAYTON, GA</td>
      <td>0.385985</td>
      <td>0.009524</td>
      <td>739.044289</td>
      <td>18.407855</td>
    </tr>
    <tr>
      <th>338</th>
      <td>FAIRFAX, VA</td>
      <td>0.360467</td>
      <td>0.010429</td>
      <td>1051.647185</td>
      <td>30.186459</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_xref.sort_values('per_area_y', ascending=False).head(10)
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
      <th>per_capita_x</th>
      <th>per_capita_y</th>
      <th>per_area_x</th>
      <th>per_area_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>246</th>
      <td>NEW YORK, NY</td>
      <td>0.529122</td>
      <td>0.068944</td>
      <td>38026.828734</td>
      <td>4908.409987</td>
    </tr>
    <tr>
      <th>203</th>
      <td>HUDSON, NJ</td>
      <td>0.411383</td>
      <td>0.179057</td>
      <td>5986.555532</td>
      <td>2559.926391</td>
    </tr>
    <tr>
      <th>330</th>
      <td>PHILADELPHIA, PA</td>
      <td>0.429204</td>
      <td>0.192800</td>
      <td>5007.382550</td>
      <td>2249.552573</td>
    </tr>
    <tr>
      <th>239</th>
      <td>KINGS, NY</td>
      <td>0.389524</td>
      <td>0.055019</td>
      <td>14458.521604</td>
      <td>2001.440271</td>
    </tr>
    <tr>
      <th>182</th>
      <td>SUFFOLK, MA</td>
      <td>0.420828</td>
      <td>0.130305</td>
      <td>5601.134996</td>
      <td>1734.153052</td>
    </tr>
    <tr>
      <th>256</th>
      <td>QUEENS, NY</td>
      <td>0.364098</td>
      <td>0.057816</td>
      <td>7819.400166</td>
      <td>1213.673639</td>
    </tr>
    <tr>
      <th>201</th>
      <td>ESSEX, NJ</td>
      <td>0.395016</td>
      <td>0.171214</td>
      <td>2491.965771</td>
      <td>1073.559940</td>
    </tr>
    <tr>
      <th>214</th>
      <td>UNION, NJ</td>
      <td>0.363034</td>
      <td>0.191182</td>
      <td>1956.761618</td>
      <td>1019.463348</td>
    </tr>
    <tr>
      <th>196</th>
      <td>BERGEN, NJ</td>
      <td>0.379372</td>
      <td>0.213033</td>
      <td>1523.398137</td>
      <td>843.920862</td>
    </tr>
    <tr>
      <th>101</th>
      <td>MARION, IN</td>
      <td>0.447889</td>
      <td>0.342362</td>
      <td>1058.721928</td>
      <td>808.846833</td>
    </tr>
  </tbody>
</table>
</div>


