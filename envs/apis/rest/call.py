import sys
from typing import Any, Union, Optional

import jsonref
import requests
from loguru import logger

# slim down response: None does no slimming
#  1 slims dicts and lists from the 1st level of nesting
#  2 slims dicts and lists from the 2nd level of nesting
SLIM_DOWN_LEVEL = 1


def slim_down_value(value: Any) -> Any:
    if isinstance(value, dict):
        # for dicts, only return 1 property
        if len(value.keys()) > 0:
            for k, v in value.items():
                return {k: slim_down_value(v)}
        else:
            return {}
    elif isinstance(value, list):
        # for lists, just return the 1st item
        if len(value) > 0:
            return [slim_down_value(value[0])]
        return []
    else:
        return value


def extract_out_dict_from_res(
        res: Any,
        out_parser: Optional[str],
        slimdownlevel: Union[int, None] = SLIM_DOWN_LEVEL
) -> Any:
    if out_parser is not None:
        if out_parser.startswith("result"):
            first_phrase = out_parser.split(".", 1)[0]
            try:
                if "[" in first_phrase and "]" in first_phrase:
                    idx = int(first_phrase.split("[")[1].replace("]", ""))
                    res = res[idx]
            except KeyError:
                if isinstance(res, dict) and "data" in res.keys():
                    return extract_out_dict_from_res(res["data"], out_parser)
                else:
                    return res
            out_parser = out_parser.split(".", 1)[1]
        while "." in out_parser:
            try:
                index = -1
                key = out_parser.split(".", 1)[0]
                if "[" in key and "]" in key:
                    index, key = (
                        int(key.split("[")[1].replace("]", "")),
                        key.split("[")[0],
                    )

                if index == -1:
                    res = res[key]
                else:
                    res = res[key][index]
                out_parser = out_parser.split(".", 1)[1]
            except BaseException as e:
                # print(e)
                if isinstance(res, dict):
                    res["status"] = False
                    res["errorMessage"] = str(e)
                else:
                    res = {"status": False, "errorMessage": str(e)}
                break

    if slimdownlevel == 1:
        slim_res = {}
        for k, v in res.items():
            slim_res[k] = slim_down_value(v)
        if "status" not in slim_res:
            slim_res["status"] = True
        return slim_res

    elif slimdownlevel == 2:
        slim_res = {}
        for k, v in res.items():
            slim_v: Any = None
            if isinstance(v, dict):
                slim_v = {}
                for k2, v2 in v.items():
                    slim_v[k2] = slim_down_value(v2)
                slim_res[k] = slim_v
            elif isinstance(v, list):
                slim_v = []
                for v2 in v:
                    slim_v.append(slim_down_value(v2))
                slim_res[k] = slim_v
            else:
                slim_res[k] = v
        if "status" not in slim_res:
            slim_res["status"] = True
        return slim_res
    if "status" not in res:
        res["status"] = True
    return res


def get_spec(base_url):

    # # URL of the OpenAPI spec endpoint on codeengine
    # openapi_url = "https://invocable-api-hub.1gxwxi8kos9y.us-east.codeengine.appdomain.cloud/openapi.json"
    # # Local URL
    # openapi_url = "http://127.0.0.1:8000/openapi.json"

    openapi_url = base_url + "/openapi.json"

    # Fetch the spec
    response = requests.get(openapi_url)

    # Check for successful response
    if response.status_code == 200:
        openapi_spec = response.json()  # Parse JSON
        logger.debug(f"OpenAPI Spec: {openapi_spec}")
    else:
        logger.info(f"Failed to fetch OpenAPI spec. Status code: {response.status_code}")
        sys.exit(1)

    return openapi_spec


def openapi_to_functions(openapi_spec):

    functions = []

    for path, methods in openapi_spec["paths"].items():
        # if not path.startswith(config["url_second_part"] + domain.split('_')[0]):
        #     continue
        # print(path.startswith(config["url_second_part"]))
        # print(path, methods)
        for method, spec_with_ref in methods.items():
            # 1. Resolve JSON references.
            spec = jsonref.replace_refs(spec_with_ref)

            # 2. Extract a name for the functions.
            function_name = spec.get("operationId")

            # 3. Extract a description and parameters.
            desc = spec.get("description") or spec.get("summary", "")

            schema = {"type": "object", "properties": {}}

            req_body = (
                spec.get("requestBody", {})
                .get("content", {})
                .get("application/json", {})
                .get("schema")
            )
            if req_body:
                schema["properties"]["requestBody"] = req_body

            params = spec.get("parameters", [])
            if params:
                param_properties = {
                    param["name"]: param["schema"]
                    for param in params
                    if "schema" in param
                }

                schema["properties"]["parameters"] = {
                    "type": "object",
                    "properties": param_properties,
                }

            functions.append(
                {
                    "name": function_name,
                    "description": desc,
                    "parameters": schema,
                    "path": path,
                }
            )

    return functions


def get_api_path_from_lookup(api_name, base_url):
    spec = get_spec(base_url)
    lookup_functions = openapi_to_functions(spec)
    for item in lookup_functions:
        if item["name"] == api_name:
            return item["path"]

    return -1


def run_tool(
    api_name: str,
    api_args: dict,
    base_url: str,
) -> Any:

    # # URL of the OpenAPI spec endpoint
    # base_url = "https://invocable-api-hub.1gxwxi8kos9y.us-east.codeengine.appdomain.cloud"
    # # Local URL
    # base_url = "http://127.0.0.1:8000"
    api_path = get_api_path_from_lookup(api_name, base_url)
    full_url = f"{base_url}{api_path}"

    try:
        response = requests.get(full_url, params=api_args)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        return response.json()  # Return parsed JSON response

    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


if __name__ == "__main__":
    _base_url = "https://invocable-api-hub.1gxwxi8kos9y.us-east.codeengine.appdomain.cloud"
    spec = get_spec(_base_url)