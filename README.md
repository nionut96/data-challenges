# backend-challenges
## Challenges
### 1. Data Architecture
There are many financial instruments and the way they are structured is complex. In this challenge, we'd like you to develop a mock database structure for financial instruments that fulfills the following sets of requirements:
* Asset Classes are an entity for financial instruments. For the purposes of this assessment, asset classes are Equities, Fixed Incomes, Cash Equivalents, Commodities and Real Estate.
* Instruments are an entity that has an ISIN (a alphanumeric ID) and a name. For the purposes of this assessment, financial instruments can be of the types: Cash, Certificate of Deposit, ETF, Futures Contract, Loan, Mortgage, Muni Bond, Mutual Fund, REITs, Stock, Treasuries. Instruments only belong to one asset class.
* Companies are an entity that include a name and description and can have multiple instruments.
* Client portfolios are an abstract organizing principle for our clients, each portfolio containing an unlimited amount of holdings. Each holding is of an instrument and has a certain percentage weight within the portfolio.
* Some instruments (Bonds, ETFs, Mutual Funds) are actually composed of other instruments, so they have a separate set of holdings themselves.
* Each instrument, each company, and each client portfolio can have an ESG score (three distinct percentiles ("E", "S", and "G") and an aggregate percentile ("score")) that needs to be tracked over time.

Your challenge is to develop a database structure that appropriately maps these relationships and requirements. Utilizing your preferred tools (e.g., MySQL, PostgreSQL, Mongo, etc.) build out this structure and provide the database code required to set it up.

### 2. Carbon Analytic Calculation
Utilizing the provided [Jupyter Notebook](/carbon_calculation/carbon_analytic_calculation.ipynb?raw=true) (and additional libraries/files/tools as needed) implement a function that can be utilized to calculate the (strawman) Adjusted CO2 Total emissions for a company. The calculation is laid out below. The data is stored inside of the [included data file](/carbon_calculation/data.json?raw=true). Your code structure can take whatever you believe to be best (individual function, class, module, etc.). Please implement the code as well as write tests for the calculation, showcasing how you would write tests for such requirements. Provide instructions with your submission for how to call your implemented function as well as how to run the tests.

![Calculation](/carbon_calculation/calculation.png?raw=true "Calculation")

## Submission
When you are ready to submit your solution, please follow these instructions:
* Fork this repo on your own GitHub account.
* Commit all of your work to the fork, including all of your source code and configuration files, as well as any compiled files.
* Submit a pull request to this repository and include any necessary instructions.
