# Covid19 Data Analysis

This program is a covid 19 Data collector/analysis program that can pull data from CSSE and OWID database and visualize it in the Kibana dashboard.

## Data Source
OWID: https://github.com/owid/covid-19-data \
CSSE: https://github.com/CSSEGISandData/COVID-19

## Installation

Installation required helm, Kubernetes cluster, and elasticsearch cluster.
 - Import Index, Map, Visualization, and Dashboard in Kibana.
 - Configure elasticsearch configuration in *values* file of helm chart.
 - Then run the following command to install the covid 19 data puller job.
```bash
git clone https://pankajackson@bitbucket.org/pankajackson/covid19dataanalysis.git
cd covid19dataanalysis
helm install covid19datapuller .
```
Then import Index, Map, Visualization, and Dashboard in Kibana

## Usage
Browse Kibana and open the *Covid19* dashboard 
![alt text](https://github.com/pankajackson/covid19dataanalysis/blob/develop/kibana/dashboard.png?raw=true)