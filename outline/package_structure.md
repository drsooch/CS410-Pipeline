# Package Structure

This is how we should envision our endproduct to look:

<pre>
package_structure
|-- external
|   |-- court_listener.py
|   `-- generic_dag.py
`-- internal
    |-- extract_operator.py
    |-- leader_operator.py
    |-- load_operator.py
    `-- transform_operator.py
</pre>

We have two folders, one for internal use (i.e. us) and one for the user.

## External:

The external folder holds two files, ```generic_dag.py``` and ```court_listener.py```.
```generic.py``` should hold a skeleton DAG of our internal operators (```Extract -> Transform -> Load```).
We should be able to provide a class or a function that returns a DAG for use in Airflow.
The class or function will accept input arguments that will change the behavior of the DAG.

For example, the user can specify: an endpoint they want, a translation from the API keys to our internal model, how often the DAG should run, etc.

The file ```court_listener.py``` will simply be a wrapper around whatever comes out of ```generic_dag.py```. This wrapper will default to the ```/opinions``` endpoint with default settings.

## Internal:

The internal folder will hold all of our custom (?) operators. They should be generic enough functions that they could feasibly be swapped out at a moments notice.
For example, the ```LeaderOperator``` should be able to point to any website, with any endpoint. This will most likely be done clientside in the airflow connections section.

The ```ExtractOperator``` should mimic the ```LeaderOperator``` in its ability to pick and choose it's endpoint.

The ```TransformOperator``` should receive some sort of translation function, dictionary, or any feasible way to transform API keys to our internal keys.

The ```LoadOperator``` should not rely on MongoDB. This is subject to change but it should be feasible to remove a line or two of code to quickly change which database it points to.

## Unknowns:

Currently, there are a few details that are up in the air. For one, the connection between each operator is currently using XCom. 
I think we should be able to leverage RabbitMQ, while simultaneously utilizing Celery to allocate enough workers to handle the incoming data.
As it stands, once the ```LeaderOperator``` runs we have no way of getting each successive operator to run _FOR EACH_ ID that comes from the API. 

