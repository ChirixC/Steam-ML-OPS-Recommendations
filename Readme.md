# Data Analysis and Game Recommendations Project on Steam

This is a project that uses machine learning to recommend games from Steam based on a user or another game. It also demonstrates how to use MLOps tools such as GitHub Actions, API, and Render to automate the workflow and deploy the model as a web app.

## Table of Contents
- Installation
- Dataset
- Usage
- Functions
- License
- Contact

## Installation

To run this project, you need to have Python 3.8 or higher installed on your machine. You also need to install the required packages using the following command:

```bash
pip install -r requirements.txt
```

## Dataset
The dataset used for this project can be found here: Steam Dataset

This project uses several datasets related to Steam and user activity on the platform. The data was originally in JSON format, we cleaned it using Code Beautify and loaded it for easier processing. This allowed us to generate different parquet files that made it easier to input data into the API.

We created the functions that would be used in the API. We also used a cosine similarity model to generate a correlation matrix between users and items. This allowed us to create the last two prediction functions.

## Usage
To run the project locally, you can use the following command:

uvicorn api:app --reload

This will launch a web app on your browser where you can interact with the model and see the predictions.

You can also access the deployed version of the web app here: Steam Recommendations MLOps

## The API has the following functions:

`/developer/{developer}`: Returns the number of items and the percentage of free content by year according to the developer company.
`/userdata/{user_id}`: Returns the amount of money spent by the user, and the percentage of recommendation.
`/userforgenre/{genre}`: Returns the user who accumulates more hours played for the given genre and a list of the accumulation of hours played by year of release for that genre.
`/bestdeveloperyear/{year}`: Returns the top 3 developers with games most recommended by users for the given year.
`/developerreviewsanalysis/{developer}`: Returns a dictionary with the developer name as key and a list with the total number of user reviews that are categorized with a sentiment analysis as positive or negative value.
To see how the model was trained and evaluated, you can open the Jupyter notebook file model.ipynb in your preferred editor.

To see how the queries were made, you can open the Jupyter notebook file ETL.ipynb in your preferred editor.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Contact
If you have any questions or feedback about this project, feel free to contact me at chirinocesar@gmail.com. You can also find me on GitHub at ChirixC.