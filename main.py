import looker_sdk
from looker_sdk import models
import configparser
import hashlib
import csv
import json
import pandas as pd

config_file = "/Users/ajager/PycharmProjects/tracking_sheet_creation/venv/looker.ini"
csv_output_path = "/Users/ajager/Desktop/new_broken_content.csv"
sdk = looker_sdk.init40(config_file)

def main():
    """Compare the output of content validator runs
    in production and development mode. Additional
    broken content in development mode will be
    outputted to a csv file.

    Use this script to test whether LookML changes
    will result in new broken content.

    Also get the number of dashboard runs & last run date"""
    base_url = get_base_url()
    folder_data = get_folder_data()
    print("Checking for broken content in production 🔎")
    broken_content_prod = parse_broken_content(
        base_url, get_broken_content(), folder_data
    )
    checkout_dev_branch()
    dev_branch = get_dev_branch_name()
    print("Checking for broken content in dev branch {} 🔎".format(dev_branch))
    broken_content_dev = parse_broken_content(
        base_url, get_broken_content(), folder_data
    )
    new_broken_content = compare_broken_content(broken_content_prod, broken_content_dev)
    dash_info = get_dash_runs(new_broken_content)

    if new_broken_content:
        print("There is new broken content in development branch {} ❌".format(dev_branch))
        """Setting Data Frames"""
        json1 = pd.DataFrame(new_broken_content)
        json1['id'] = json1['id'].astype('int') #Converts id to an int in order to merge
        json2 = pd.DataFrame(dash_info).set_index('dashboard.id')
        merged = pd.merge(json1, json2, how='left', left_on='id', right_on='dashboard.id')#.to_dict(orient='records')
        """Setting run count to "look" or 0"""
        merged.loc[merged['content_type'] == "look", "history.dashboard_run_count"] = "NA - Look"
        merged.loc[merged['history.dashboard_run_count'] == None, "history.dashboard_run_count"] = 0
        write_broken_content_to_file(merged.to_dict(orient='records'), csv_output_path)
    else:
        print("No new broken content in development branch {} ✅".format(dev_branch))


def get_base_url():
    """Pull base url from looker.ini, remove port"""
    config = configparser.ConfigParser()
    config.read(config_file)
    full_base_url = config.get("Looker", "base_url")
    base_url = sdk.auth.settings.base_url[: full_base_url.index(":19999")]
    return base_url


def get_folder_data():
    """Collect all folder information"""
    folder_data = sdk.all_folders(fields="id, parent_id, name")
    return folder_data


def get_broken_content():
    """Collect broken content"""
    broken_content = sdk.content_validation(
        transport_options={"timeout": 600}
    ).content_with_errors
    return broken_content


def parse_broken_content(base_url, broken_content, folder_data):
    """Parse and return relevant data from content validator"""
    output = []
    for item in broken_content:
        if item.dashboard:
            content_type = "dashboard"
        else:
             content_type = "look"
        item_content_type = getattr(item, content_type)
        id = item_content_type.id
        name = item_content_type.title
        folder_id = item_content_type.folder.id
        folder_name = item_content_type.folder.name
        errors = item.errors
        error_list = errors[0]
        error_message = error_list.message
        error_model = error_list.model_name
        error_explore = error_list.explore_name
        url = f"{base_url}/{content_type}s/{id}"
        folder_url = "{}/folders/{}".format(base_url, folder_id)
        if content_type == "look":
            element = "Look Name = {}".format(item_content_type.title)
        elif item.dashboard_filter:
            element = "Filter Name = {}".format(item.dashboard_filter.name)
        else:
            dashboard_element = item.dashboard_element
            element = dashboard_element.title if dashboard_element else None
        # Lookup additional folder information
        folder = next(i for i in folder_data if str(i.id) == str(folder_id))
        parent_folder_id = folder.parent_id
        if parent_folder_id is None or parent_folder_id == "None":
            parent_folder_url = None
            parent_folder_name = None
        else:
            parent_folder_url = "{}/folders/{}".format(base_url, parent_folder_id)
            parent_folder = next(
                (i for i in folder_data if str(i.id) == str(parent_folder_id)), None
            )
            # Handling an edge case where folder has no name
            try:
                parent_folder_name = parent_folder.name
            except AttributeError:
                parent_folder_name = None
        # Create a unique hash for each record. This is used to compare
        # results across content validator runs
        unique_id = hashlib.md5(
            "-".join(
                [str(id), str(element), str(name), str(errors), str(folder_id)]
            ).encode()
        ).hexdigest()
        if item.dashboard_filter:
            is_filter = "Yes"
        else:
            is_filter = "No"
        if item.alert:
            has_alert = "Yes"
        else:
            has_alert = "No"
        if item.scheduled_plan:
            is_scheduled = "Yes"
        else:
            is_scheduled = "No"
        data = {
            "unique_id": unique_id,
            "id": id,
            "content_type": content_type,
            "content_name": name,
            "url": url,
            "element": element,
            "is_filter": is_filter,
            "has_alert": has_alert,
            "is_scheduled": is_scheduled,
            "folder_name": folder_name,
            "folder_url": folder_url,
            "parent_folder_name": parent_folder_name,
            "parent_folder_url": parent_folder_url,
            "error_message": error_message,
            "error_explore": error_explore,
            "error_model": error_model
        }
        output.append(data)
    return output


def compare_broken_content(broken_content_prod, broken_content_dev):
    """Compare output between 2 content_validation runs"""
    unique_ids_prod = set([i["unique_id"] for i in broken_content_prod])
    unique_ids_dev = set([i["unique_id"] for i in broken_content_dev])
    new_broken_content_ids = unique_ids_dev.difference(unique_ids_prod)
    new_broken_content = []
    for item in broken_content_dev:
        if item["unique_id"] in new_broken_content_ids:
            new_broken_content.append(item)
    return new_broken_content


def checkout_dev_branch():
    """Enter dev workspace"""
    sdk.update_session(models.WriteApiSession(workspace_id="dev"))

def get_dev_branch_name():
    """Getting dev branch name"""
    response = sdk.git_branch('attentivemobile')
    name_loc = str(response).find('name=')
    remote_loc = str(response).find('remote=')
    branch_name = str(response)[name_loc+5:remote_loc-2]
    return branch_name


def write_broken_content_to_file(broken_content, output_csv_name):
    """Export new content errors in dev branch to csv file"""
    try:
        with open(output_csv_name, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(broken_content[0].keys()))
            writer.writeheader()
            for data in broken_content:
                writer.writerow(data)

        """Sorting csv by id"""
        csv_p = pd.read_csv(output_csv_name)
        csv_p.sort_values(['id'],
                     axis=0,
                     ascending=[True],
                     inplace=True)
        csv_sorted = pd.DataFrame(csv_p)
        csv_sorted.to_csv(output_csv_name, index=False)
        print("Broken content information outputed to {}".format(output_csv_name))
    except IOError:
        print("I/O error - File given does not exist")

def get_dash_runs(list):
    """Get dashboard ids from list of broken content"""
    dash_id_list = []
    for item in list:
        if item["content_type"] == "dashboard":
            dash_id_list.append(item["id"])

    """Running look to get runs"""
    look_id = "4224"
    look = sdk.look(look_id=look_id)
    run_query = look.query

    string_list = ",".join(dash_id_list)

    filter_field = "dashboard.id"
    filter_values = string_list
    run_query.filters[filter_field] = filter_values

    run_query.client_id = None
    run_query.id = None
    new_query = sdk.create_query(body=run_query)
    response = sdk.run_query(query_id=new_query.id, result_format="json")
    return_data = json.loads(response)
    return return_data




main()


