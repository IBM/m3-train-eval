import copy
from functools import wraps
from typing import Callable,Tuple,Dict

class AgenticTracker:
    """track and log agent output and metadata, reasoning trajectory"""

    def __init__(self):
        # Store logs of inputs and outputs
        self.logs = []
        self.relation_graph = None

    def log(self, func_name, output, role="", tracking_result=None):
        """Logs function name, inputs, and output."""
        self.logs.append({
            'function': func_name,
            'output': output,
            "role": role,
            "tracking_result": tracking_result,
            "trajectory": []
        })

    def track_agent(self, func:Callable):
        """Decorator to track inputs and outputs of a function."""

        @wraps(func)
        def wrapper(*args, **kwargs) -> Tuple[str,Dict]:
            # Convert the arguments into a string for logging
            # inputs = {'args': args, 'kwargs': kwargs}
            # Call the function and capture its result
            result = func(*args, **kwargs)
            tracking_result = result["tracking_result"]
            output_text = result["output_text"]
            output_meta = result["output_meta"]
            role = result["role"]
            # Log the function name, inputs, and output
            self.log(func.__name__, output_text, role=role, tracking_result=tracking_result)
            return output_text, output_meta

        return wrapper

    def get_role_destination(self, role: str, relation_graph: dict, parent_role: str = "main") -> str:
        """
        Based on relatonal graph, determine destination of current role
        :param role:
        :param relation_graph: json dict
        :param parent_role: current role's parent role
        :return: destination role
        """
        if len(relation_graph) == 0:
            return None
        if role in relation_graph:
            if parent_role == "main":
                return "main_" + role
            else:
                return parent_role
        else:
            for k in relation_graph:
                sub_relation_graph = relation_graph[k]
                res = self.get_role_destination(role, sub_relation_graph, parent_role=k)  # nested relation
                if res is not None:
                    return res

    def assign_traj(self, source: dict, target: dict) -> bool:
        """
        try to(may not) assign source record into the target record trajectory, based on destination field of the source
        :param source:  dict record of source
        :param target: dict record of target
        :return: bool status of assignment, if they matched return true, else return false
        """
        s_destination = self.get_role_destination(source["role"], self.relation_graph)
        if s_destination == target["role"]:
            target["trajectory"].append(source)
            return True
        else:
            for t in target["trajectory"]:
                if self.assign_traj(source, t):  # nested trajectory
                    return True
        return False

    def record2log(self, r: dict) -> dict:
        """
        convert record format into log format
        :param r: record
        :return: log
        """
        tmp = {"role": r["role"], "content": r["output"], "meta": r["tracking_result"]}
        tmp["destination"] = self.get_role_destination(tmp["role"], self.relation_graph)
        tmp["trajectory"] = []
        return tmp

    def get_structurize_logs(self):
        """
        maintain a task list(list ofrecords) to process,until all records are processed and assigned based on destination of the role.
        :return: generated logs data based on records of agent output.
        """
        logs_copy = copy.deepcopy(self.logs)
        agentic_logs = []
        while len(logs_copy) > 0:
            for l in logs_copy:
                fine_l = self.record2log(l)
                destination = self.get_role_destination(fine_l["role"], self.relation_graph)
                if "main_" in destination:  # main log(entry)
                    agentic_logs.append(fine_l)
                    logs_copy.remove(l)
                    break
                else:
                    if len(agentic_logs) > 0:
                        if self.assign_traj(fine_l, agentic_logs[-1]):  # assign to main log(entry)
                            logs_copy.remove(l)
                            break
        return agentic_logs

    def empty_logs(self):
        self.logs = []

    def set_relation_graph(self, graph: dict):
        self.relation_graph = graph