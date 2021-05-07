import os

from airflow.providers.mongo.hooks.mongo import MongoHook

def local_batch(
    batch_name: str,
    max_batch_size: int,
    number_of_batches: int,
    root_path: str
    ) -> None:

    curr_item_num = 0
    batch_id = 1
    id_list = []

    mongo_conn = MongoHook(conn_id="default_mongo")

    # Iterate through all directories and assign each filepath a batch_id.
    for root, directories, files in os.walk(root_path, topdown=False):
        for name in files:
            fp = os.path.join(root, name)
            if curr_item_num == max_batch_size:

                mongo_conn.insert_many("local_results_to_transform", id_list, mongo_db="courts")

                curr_item_num = 0
                batch_id %= number_of_batches
                batch_id += 1
                id_list = []

            if curr_item_num < max_batch_size:

                # Each document in local_results_to_transform is a dict of
                # the filepath and the batch it is assigned to.
                id_list.append(
                    {
                        "batch_id" : f"{batch_name}{batch_id}",
                        "file_path" : fp
                    }
                )
                curr_item_num += 1

    # Push any remaining filepaths to the local_results_to_transform collection.
    if (curr_item_num > 0):
        mongo_conn.insert_many(
            "local_results_to_transform", id_list, mongo_db="courts"
        )
        id_list = []
