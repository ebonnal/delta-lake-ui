
# DeltaLakeUI (State: POC)
Minimalist web UI to run SQL on [Delta Lake](https://github.com/delta-io/delta) tables and visualize the variations of the result among tables versions.

## Architecture
- Minimal HTML/CSS/JS Front where user enter SQL queries
- Basic [Flask](https://github.com/pallets/flask) server
- [Spark](https://github.com/apache/spark) used to query tables versions
- [Pygal](https://github.com/Kozea/pygal) used to plot result of the query over versions of tables
<img width="600px" src="https://github.com/EnzoBnl/DeltaLakeUI/blob/master/screens/archi.png"></img>
## Screens
<img width="600px" src="https://github.com/EnzoBnl/DeltaLakeUI/blob/master/screens/1.png"></img>
<img width="600px" src="https://github.com/EnzoBnl/DeltaLakeUI/blob/master/screens/2.png"></img>
