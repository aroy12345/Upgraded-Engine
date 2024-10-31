# Capybara Search
This repository contains the codebase for Project Capybara. Below is an overview of the structure and key components of the project:

![image](https://github.com/CIS5550/Sp24-CIS5550-Project-capybara/assets/76747388/0cda1371-7e46-4457-b464-5e4529ffa7f6)


# Structure
src: All code files are located in this directory.
Branches
Each integrated part of the project is separated into its own branch for better organization and development isolation.

## crawler:
Contains batch processing implementations to mitigate network latency issues and improve EC2 performance and other interesting optimization.
## indexer:
Implements functionality similar to what was done in HW9. FromTable, Flatmap and fold.
## pagerank: 
Implements the Pagerank algorithm, also based on the HW9 implementation. Handles dead links by creating self-loops or ignoring them so all links reach convergence.
## combine-scores: 
Implements a function to assign scores to each page based on TF-IDF and Pagerank. This is essentially an enhanced version of the HW9 implementation, optimized for parallelization.

# Key Features
## Batch Processing: 
Implemented batch processing techniques in the crawler to address network latency issues and optimize EC2 performance.
## Data Preprocessing:
Removed stop words, HTML tags, and uncaught div elements to reduce storage size and enhance data cleanliness.
## Scoring:
The combine scores function assigns scores to each page based on TF-IDF and Pagerank, providing a comprehensive measure of relevance.
## Autocomplete:
Implemented with jquery on the user input in the search bar
## Suggested Search
JaroWinkler distance computes closest match word(s) given typo'd user input into the search bar. 
## Website Images
Includes the logo of the website next to search results.
## Search time
Outputs the time it took to complete a search query.

# Usage
bash launch_workeres.sh or crawler.bat then submit the jobs using the flameSubmit api for each of the components in jobs (Crawler, Indexer, Pagerank, Combine Scores, TF_IDF)

For the front end and backend, simply call the executable class file after compiling. You should also launch_workers.sh before calling invoking these classes.

