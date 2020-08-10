# Predicting Average Adjusted Gross Income (AGI)

## Goal: Develop linear regression model (Spark ML) on NYU DUMBO to Predict Average Adjusted Gross Income (AGI)

### Input

- Counts of businesses by NAICS type and size

- Demographics (gender, race)

- Educational Attainment

### Output

- Average AGI (total AGI / total population)

### 4 Datasets Used

- U.S. Census County Business Patterns (CBP)

- U.S. Census American Community Survey (ACS)

	- Demographics & Educational Attainment

- U.S. IRS SOI Tax Stats - County Data

### Results

- Model fitted on years 2011 to 2016, evaluated on 2017

- 91% of predications fall within 30% of the target variable

- RMSE of 5.5, R2 value of 0.63

- Predictions and coefficients displayed on Tableau for user interaction
