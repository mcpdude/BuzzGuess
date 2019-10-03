# Unherd

Don't miss customer's frustrations.


Unherd is a tool to produce a easily queryable database of highly similar comments.

Once it's set up, it runs a pair of intake scripts on a user configured basis. The input data is stored in the PostgreSQL database. On a separate interval, the process script creates or loads a bag of words, then hashes the stored tables to create a new, results table with individual sentences. The sentences are sorted by increasing Jaccard similarity.

Since the intake and process scripts are decoupled, it's recommended to use a smaller cluster for intake, and a larger, more capable cluster for the processing stage. Because the processing stage can be run independently of the intake, it's advised to spin up and spin down the process cluster in between analyses.

