First part of the project - 
1) take data from api stock data, the data is taken for each day.
2) take the metadata for all the stock in usa, this is a one time process, the data will be stored in s3
3) enrichment of the stock_data
4) in parallel get the data for sentiment on article data
5) make a coorellation between sentiment on the articles and the stock behavior on the time
