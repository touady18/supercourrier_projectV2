 # SuperCourier project - Mini ETL Delivery Delay Pipeline 
 superCourier is a mini pepline coded in Python that simulates delivery activity by taking several factors into account (delivery zone, weather, day of the week, etc.).

## Prérequis

- Python 3.8+  
- Modules Python :
  - `pandas`
  - `numpy`

 ## Local installation

 ## Clone the project
 ```bash
git clone https://github.com/touady18/supercourrier_projectV2.git
cd supercourier_project
```

## Launch the pipeline
python de-code-snippet.py


# Main functions
## generate_weather_data()
Create a JSON file with weather per hour for 3 months.

## extract_sqlite_data() & load_weather_data()
Load the data to a dataframe 

## transform_data()
Transform the data and calculate the delivery times

## save_results()
Save the result into a csv file
deliveries.csv