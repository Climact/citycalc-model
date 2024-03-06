# ----------------------------------------------------------------------------------------------------- #
# 2020, Climact, Louvain-La-Neuve
# ----------------------------------------------------------------------------------------------------- #
# __  __ ____     _     _      ____
# \ \/ // ___|   / \   | |    / ___|
#  \  /| |      / _ \  | |   | |
#  /  \| |___  / ___ \ | |___| |___
# /_/\_\\____|/_/   \_\|_____|\____| - X Calculator project
#
# ----------------------------------------------------------------------------------------------------- #

from copy import copy
import logging
import os
import re
import xml.etree.cElementTree as ET
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
import inspect
from inspect import getframeinfo, stack
import typing

import pandas as pd


FlowVars = dict
Flow = typing.Any
FlowStr = str
FlowInt = int
FlowFloat = float


def dbg(x, title=None):
    if title is not None:
        print(f"{title} = {x}")
    else:
        print(x)
    return x


@dataclass
class KnimeNodeId:
    """An identifier for a node imported from KNIME."""

    # The actual node ID
    id: str
    # The XML file with the settings for this node
    xml_file: str


class Globals:
    local: str
    base_year: int
    max_year: int
    country_filter: str
    levers: dict[str, float]
    dynamic_levers: dict[str, typing.Any]
    missing_years: list[int]

    def __init__(
        self,
        local: str,
        base_year: int,
        max_year: int,
        country_filter: str,
        levers: dict[str, float],
        dynamic_levers: dict[str, typing.Any],
    ):
        self.local = local
        self.base_year = base_year
        self.max_year = max_year
        self.country_filter = country_filter
        self.levers = levers
        self.dynamic_levers = dynamic_levers

        # TODO don't hardcode this
        PATH_YEARS_LIST = Path(self.local, "_common", "reference", "ref_years.xlsx")
        ref_years = pd.read_excel(PATH_YEARS_LIST)
        self.missing_years = list(
            ref_years[ref_years["cds_optional"]]["Years"].astype(int)
        )

    @classmethod
    def get(cls):
        global _globals
        assert _globals, "Globals not set"
        return _globals[-1]
    
    def __enter__(self):
        Globals.push(self)
        return self
    
    def __exit__(self, *args):
        Globals.pop()
    
    @classmethod
    def push(cls, globals):
        global _globals
        _globals.append(globals)
    
    @classmethod
    def pop(cls):
        global _globals
        _globals.pop()


_globals = []


class Context:
    def __init__(
        self, local,
        module_name: str | None = None
    ):
        self.local = local
        self.module_name = module_name


class NativeNode(ABC):
    knime_name: str
    knime_type = "NativeNode"


class SubNode(ABC):
    knime_name = "Wrapped Node"
    knime_type = "SubNode"


class PythonNode:
    def __init__(self):
        # # Caller information, to help identify this node
        # caller = getframeinfo(stack()[1][0])
        # self.location = f"{caller.filename}:{caller.lineno}"
        # FIXME: computing location from stack frame is too slow, and we don't really need it.
        #   We should remove it from wherever it's used.
        self.location = ""
        self.in_ports = {}
        self.out_ports = {}
        self.flow_vars_params = {}
        self.flow_vars = {}
        self.was_run = False
        self.uses_flow_vars = False
        if hasattr(self, "apply"):
            self.knime_input_mapping = {}
            self.knime_output_mapping = {}
        self.init_ports()
        self.init_port_mapping()

    def __call__(
        self,
        *args: tuple[typing.Any],
        **kwargs: dict[str, typing.Any],
    ):
        return self.apply(
            *[copy(arg) for arg in args],
            **{key: copy(kwarg) for key, kwarg in kwargs.items()}
        )

    def n_outputs(self):
        annot = typing.get_type_hints(self.apply)
        if "return" not in annot:
            raise NotImplementedError
        else:
            ret = annot["return"]
            if isinstance(ret, typing.GenericAlias) and ret.__origin__ == tuple:
                return len(ret.__args__)
            else:
                return 1

    def init_port_mapping(self):
        if not hasattr(self, "apply"):
            return

        annots = typing.get_type_hints(self.apply)

        if "return" in annots:
            ret = annots["return"]
            scalar = False
            if isinstance(ret, typing.GenericAlias) and ret.__origin__ == tuple:
                for i, ty in enumerate(ret.__args__):
                    if i not in self.knime_output_mapping:
                        if ty == dict:
                            self.knime_output_mapping[i] = "__flow_vars__"
                        else:
                            self.knime_output_mapping[i] = i + 1
            else:
                if ret == dict:
                    self.knime_output_mapping = "__flow_vars__"
                else:
                    self.knime_output_mapping = 1

        sig = inspect.signature(self.apply)
        for i, (name, param) in enumerate(sig.parameters.items()):
            if param.default != inspect._empty:
                continue
            if name in self.knime_input_mapping:
                continue

            ty = annots.get(name, None)
            if ty == dict:
                mapping = "__flow_vars__"
            elif ty in [str, int, float, typing.Any]:
                mapping = name
            else:
                mapping = i + 1
            if param.kind != inspect._VAR_KEYWORD:
                self.knime_input_mapping[name] = mapping

    @staticmethod
    def init_wrapper(cls, ctx = None, /, **kwargs):
        if ctx is None:
            self = cls(**kwargs)
        else:
            self = cls(ctx, **kwargs)
        self._params = kwargs
        return self

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        self = PythonNode.init_wrapper(cls)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def init_knime(
        self,
        ctx: Context,
        id: str,
        xml_root: ET.Element,
        xmlns: str,
        flow_vars_params: dict | None,
    ):
        self.id = id
        # TODO add the XML file path somehow
        self.location = f"Node #{id}"
        if flow_vars_params is not None:
            self.flow_vars_params = flow_vars_params
            self.uses_flow_vars = True

    def find_used_variables(self, subtree, path, used_variables):
        """Recursively reads the xml subtree, finds all the flow variables in use, returns
            a list containing the names of the variables and their paths in the xml tree

        :param subtree:
        :param path:
        :param used_variables:
        :return:
        """
        if subtree.get("key") != "used_variable":
            if subtree.get("key") != "variables":
                path = path + "/" + subtree.get("key")

            for child in subtree:
                self.find_used_variables(child, path, used_variables)

        else:
            used_variables.append((path, subtree.get("value")))

    def run(self):
        flow_vars = self.flow_vars
        del self.flow_vars

        def read_input(key: int | str):
            if isinstance(key, int):
                return self.read_in_port(key)
            elif isinstance(key, str):
                if key == "__flow_vars__":
                    return flow_vars.copy()
                else:
                    try:
                        return flow_vars[key]
                    except KeyError:
                        raise KeyError(
                            f"{self.__class__.__name__}: no flow variable `{key}` passed in"
                        )
            else:
                raise ValueError(
                    f"{self.__class__.__name__}: invalid input mapping: `{key}`"
                )

        def save_output(key: int | str, value):
            if isinstance(key, int):
                self.save_out_port(key, value)
            elif isinstance(key, str):
                if key == "__flow_vars__":
                    flow_vars.update(value)
                else:
                    flow_vars[key] = value
            else:
                raise ValueError(
                    f"{self.__class__.__name__}: invalid output mapping: `{key}`"
                )

        outputs = self.apply(
            **{
                key: read_input(value)
                for key, value in self.knime_input_mapping.items()
            }
        )
        if isinstance(self.knime_output_mapping, dict):
            for i, output in enumerate(outputs):
                try:
                    save_output(self.knime_output_mapping[i], output)
                except KeyError:
                    raise KeyError(
                        f"{self.__class__.__name__}: no output #{i} configured"
                    )
        else:
            save_output(self.knime_output_mapping, outputs)

        self.flow_vars = flow_vars

    def run_timed(self):
        start = timer()
        self.debug(f"START")

        self.run()

        t = timer() - start
        self.debug(f"END ({t:8.4f} s)")

    def init_ports(self):
        pass

    def create_0_in_port(self):
        self.in_ports[0] = None

    def create_0_out_port(self):
        self.out_ports[0] = None

    def reset_ports(self, port_id, data_io):
        """Reset nodes port (set values to None)

        :param port_id: input or output port number
        :param data_io: in or out
        :return: None
        """
        if data_io == "out":
            self.out_ports[port_id] = None
        elif data_io == "in":
            self.in_ports[port_id] = None
        else:
            raise self.exception("Unknown data_io type")

    def exception(self, msg: str):
        """Create an exception with this node's location and name"""
        raise Exception(f"{self.location} ({self.knime_name}): {msg}")

    def error(self, msg: str):
        """Log an error message with this node's location and name"""
        logging.error(f"{self.location} ({self.knime_name}): {msg}")

    def debug(self, msg: str):
        """Log a debug message with this node's location and name"""
        logging.debug(f"{self.location} ({self.knime_name}): {msg}")

    def warning(self, msg: str):
        """Log a warning message with this node's location and name"""
        logging.warning(f"{self.location} ({self.knime_name}): {msg}")

    @classmethod
    def knime_exception(cls, id, msg: str):
        """Create an exception with this node's location and name

        Alternative to `exception` that exists on the class itself, for use in
        `from_knime`.
        """
        raise Exception(f"Node #{id} ({cls.knime_name}): {msg}")

    @classmethod
    def knime_error(cls, id, msg: str):
        """Log an error message with this node's location and name

        Alternative to `error` that exists on the class itself, for use in
        `from_knime`.
        """
        logging.error(f"Node #{id} ({cls.knime_name}): {msg}")

    @classmethod
    def knime_debug(cls, id, msg: str):
        """Log a debug message with this node's location and name

        Alternative to `debug` that exists on the class itself, for use in
        `from_knime`.
        """
        logging.debug(f"Node #{id} ({cls.knime_name}): {msg}")

    @classmethod
    def knime_warning(cls, id, msg: str):
        """Log a warning message with this node's location and name

        Alternative to `warning` that exists on the class itself, for use in
        `from_knime`.
        """
        logging.warning(f"Node #{id} ({cls.knime_name}): {msg}")


def patternreshape(pattern, case_sensitive="false"):
    """One of the biggest challenge in the converter is to use the REGEX of the Knime workflow the same way in
    the converter. The pattern reshape function allows to reshape the regex pattern in order to make that possible

    :param pattern: REGEX pattern
    :param case_sensitive: boolean
    :return: reshaped pattern
    """

    # Knime and python are dealing differently with the REGEX
    if pattern.startswith(" "):
        # Remove empty space at the beginning of the pattern
        pattern = pattern[1:]
    if pattern.startswith("|"):
        # Remove 'OR' character at the beginning of the pattern
        pattern = pattern[1:]
    if pattern.endswith("|"):
        # Remove 'OR' character at the end of the pattern
        pattern = pattern[:-1]
    str_start = "^"  # For the regex to only look at the beginning of the word
    str_end = "$"  # For the regex to look at the whole word
    str_neg = "(?!"  # To avoid entering in negative lookahead
    str_bracket = "("  # To raise exception if brackets
    if str_neg not in pattern:
        if str_bracket in pattern:
            str_between_bracket = pattern[pattern.find("(") + 1 : pattern.find(")")]
            if "|" in str_between_bracket:
                raise ValueError(
                    'Remove the bracket and the OR char in the pattern ("'
                    + pattern
                    + '")'
                )
        pattern_split = pattern.split("|")
        # insert starting and ending string for each item of the pattern separated by OR char
        pattern_split = [str_start + item + str_end for item in pattern_split]
        pattern = "|".join(pattern_split)
    else:
        pattern = str_start + pattern + str_end
    if case_sensitive == "true":
        pattern = re.compile(pattern)
    else:
        pattern = re.compile(pattern, re.IGNORECASE)
    return pattern


class Node(ABC):
    def __init__(self, id, xml_file, node_type, knime_workspace=None, module_name=None):
        if self.__class__.__name__ not in [
            "MetaNode",
            "WrappedMetaNode",
            "MetaconnectorNode",
            "TempNode",
            "QualityCheckNode",
        ]:
            raise RuntimeError(
                f"node `{self.__class__.__name__}` has not been converted for the upcoming Python model"
            )

        self.logger = logging.getLogger(__name__)
        self.id = id
        self.xml_file = xml_file
        self.xml_root = self.read_xml_file()
        self.xmlns = "{http://www.knime.org/2008/09/XMLConfig}"
        self.in_ports = {}
        self.out_ports = {}
        self.knime_type = node_type
        self.knime_name = self.get_node_name()
        if knime_workspace is None:
            self.local = os.path.join("E:\\EUCalc", "knime2python")
        else:
            self.local = knime_workspace
        self.was_run = False
        self.uses_flow_vars = False
        self.flow_vars_params = {}
        self.flow_vars = {}
        self.module_name = module_name
        if self.knime_type == "MetaNode":
            self.graph = None
        if self.knime_type == "SubNode":
            self.supernode = None
            self.version_number = 0  # All wrapped metanodes have a default version number of 0, the EUCalc - Tree nodes
            #  specify their version numbers in wrapped node input's description
        # ------------------------------------------------------------------ #
        # Initialisation functions (need to stay at the end of the __init__)
        # ------------------------------------------------------------------ #
        self.init_ports()
        self.prepare_node_for_run()
        self.build_node()

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def init_ports(self):
        pass

    @abstractmethod
    def build_node(self):
        pass

    def read_xml_file(self):
        tree = ET.ElementTree(file=self.xml_file)
        root = tree.getroot()
        return root

    def get_node_name(self):
        """Get the name of the node based on node type

        :return: node name
        """
        if self.knime_type == "NativeNode":
            # Standard Knime nodes
            node_name = self.xml_root.find(self.xmlns + "entry[@key='node-name']").get(
                "value"
            )
        elif self.knime_type == "SubNode":
            # Components (wrapped metanodes)
            node_name = "Wrapped Node"
        elif self.knime_type == "MetaNode":
            # Standard metanode
            node_name = "MetaNode"
        elif self.knime_type == "TempNode":
            node_name = "TempNode"
        else:
            raise Exception("Unknown knime node type: {0}".format(self.knime_type))
        return node_name

    def load_in_port(self, id, df):
        """Load dataframe in specific input port of the node

        :param id: input port number
        :param df: dataframe
        :return: None
        """
        # FIXME: the below test was removed as it was causing issues in the UPDATE loop (adapt the conditions)
        # if id in self.in_ports and self.in_ports[id] is not None:
        #    raise Exception("in port already loaded for node {0}".format(self.id))
        # TODO: Add out of range exception (to avoid trying to load a port that does not exist)
        self.in_ports[id] = df

    def read_in_port(self, id: object) -> object:
        """Read data from specific input port of the node

        :param id: input port number
        :return: data
        """
        if id not in self.in_ports:
            raise Exception("in port not loaded yet for node {0}".format(self.id))
        return self.in_ports[id]

    def read_out_port(self, id):
        """Read data from specific output port of the node

        :param id: output port number
        :return: data
        """
        if id not in self.out_ports:
            raise Exception("out port not loaded yet for node {0}".format(self.id))
        return self.out_ports[id]

    def save_out_port(self, id, df):
        """Save dataframe in specific output port of the node

        :param id: output port number
        :param df: dataframe
        :return: None
        """
        if id in self.out_ports and self.out_ports[id] is not None:
            raise Exception("out port already loaded for node {0}".format(self.id))
        # TODO: Add out of range exception (to avoid saving data in a non existing port)
        self.out_ports[id] = df

    def reset_ports(self, port_id, data_io):
        """Reset nodes port (set values to None)

        :param port_id: input or output port number
        :param data_io: in or out
        :return: None
        """
        if data_io == "out":
            self.out_ports[port_id] = None
        elif data_io == "in":
            self.in_ports[port_id] = None
        else:
            raise Exception(
                "Unknown data_io type (node #"
                + self.id
                + ". xml:"
                + self.xml_file
                + ")"
            )

    def create_0_in_port(self):
        self.in_ports[0] = None

    def create_0_out_port(self):
        self.out_ports[0] = None

    def log_timer(self, t, fun, node_name=None):
        """Log the timer and the Name of all the nodes in Debug mode

        :param t: timing (in [s])
        :param fun: function of the converter (BUILD, RUN or UPDATE)
        :param node_name: name of the node
        :return: None
        """
        if node_name is None:
            node_name = self.knime_name
        counter = fun.count("#")

        # if counter == 3 is a quick way to find the main metanodes to be able to display them differently from the
        # other nodes
        if counter == 3:
            # if there is no timing, it means we are a the beginning of the node
            if t is None:
                self.logger.debug("{:30s}: {:5s}".format(fun, node_name))
            else:
                self.logger.info("{:30s}: {:5s} ({:8.4f} s )".format(fun, node_name, t))
        if counter > 3:
            if t is None:
                self.logger.debug("{:30s}: {:5s}".format(fun, node_name))
            else:
                self.logger.debug(
                    "{:30s}: {:5s} ({:8.4f} s )".format(fun, node_name, t)
                )
        else:
            if t is None:
                self.logger.debug(
                    "{:19s}: {} #{} - XML: {}".format(
                        fun, node_name, self.id, self.xml_file
                    )
                )
            else:
                self.logger.debug(
                    "{:5s} ({: 8.4f}[s]): {} #{} - XML: {}".format(
                        fun, t, node_name.encode("utf8"), self.id, self.xml_file
                    )
                )

    def patternreshape(self, pattern, case_sensitive="false"):
        """One of the biggest challenge in the converter is to use the REGEX of the Knime workflow the same way in
        the converter. The pattern reshape function allows to reshape the regex pattern in order to make that possible

        :param pattern: REGEX pattern
        :param case_sensitive: boolean
        :return: reshaped pattern
        """

        # Knime and python are dealing differently with the REGEX
        if pattern.startswith(" "):
            # Remove empty space at the beginning of the pattern
            pattern = pattern[1:]
        if pattern.startswith("|"):
            # Remove 'OR' character at the beginning of the pattern
            pattern = pattern[1:]
        if pattern.endswith("|"):
            # Remove 'OR' character at the end of the pattern
            pattern = pattern[:-1]
        str_start = "^"  # For the regex to only look at the beginning of the word
        str_end = "$"  # For the regex to look at the whole word
        str_neg = "(?!"  # To avoid entering in negative lookahead
        str_bracket = "("  # To raise exception if brackets
        if str_neg not in pattern:
            if str_bracket in pattern:
                str_between_bracket = pattern[pattern.find("(") + 1 : pattern.find(")")]
                if "|" in str_between_bracket:
                    self.logger.error(
                        'Remove the bracket and the OR char in the pattern ("'
                        + pattern
                        + '") of your '
                        + self.knime_name
                        + " node (#"
                        + self.id
                        + ". xml:"
                        + self.xml_file
                        + ")"
                    )
            pattern_split = pattern.split("|")
            # insert starting and ending string for each item of the pattern separated by OR char
            pattern_split = [str_start + item + str_end for item in pattern_split]
            pattern = "|".join(pattern_split)
        else:
            pattern = str_start + pattern + str_end
        if case_sensitive == "true":
            self.pattern = re.compile(pattern)
        else:
            self.pattern = re.compile(pattern, re.IGNORECASE)
        return self.pattern

    def prepare_node_for_run(self):
        """Checks if node is controlled by any flow variables, if so,
            the xml settings tree is modified so the controlled parameters values are set by the
            flow variables

        :return: None
        """
        # FIXME: redundancy of this function with the prepare_node_for_run in the workflow_runner (remove this one)
        if self.xml_root is not None:
            variables = self.xml_root.find(self.xmlns + "config[@key='variables']")

            if variables is not None:
                self.uses_flow_vars = True
                self.logger.debug(
                    "The node #"
                    + str(self.id)
                    + " ("
                    + self.knime_name
                    + ") is using Flow Variables to control the parameters. The XML file will be modified "
                    "to use this flow variable in the node."
                )

                used_variables = []
                self.find_used_variables(variables, "", used_variables)
                model_in_settings_file = self.xml_root.find(
                    self.xmlns + "config[@key='model']"
                )

                for used_variable in used_variables:
                    (path, var) = used_variable
                    keys = path.split("/")
                    keys.pop(0)  # Get rid of the useless '' entry at index 0

                    # In the metanode template, there is a 'tree' key that does not exist in the 'model' entry.
                    # We remove it.
                    if "tree" in keys:
                        keys.remove("tree")
                    parameter_to_modify = model_in_settings_file

                    for key in keys:
                        if (
                            parameter_to_modify.find(
                                self.xmlns + "config[@key='" + key + "']"
                            )
                            is not None
                        ):
                            parameter_to_modify = parameter_to_modify.find(
                                self.xmlns + "config[@key='" + key + "']"
                            )
                        else:
                            parameter_to_modify = parameter_to_modify.find(
                                self.xmlns + "entry[@key='" + key + "']"
                            )
                    if parameter_to_modify is not None:
                        parameter_to_modify.attrib["value"] = var
                        self.flow_vars_params[key] = var

    def find_used_variables(self, subtree, path, used_variables):
        """Recursively reads the xml subtree, finds all the flow variables in use, returns
            a list containing the names of the variables and their paths in the xml tree

        :param subtree:
        :param path:
        :param used_variables:
        :return:
        """

        if subtree.get("key") != "used_variable":
            if subtree.get("key") != "variables":
                path = path + "/" + subtree.get("key")

            for child in subtree:
                # print(ET.tostring(child, encoding='utf8').decode('utf8'))
                self.find_used_variables(child, path, used_variables)

        else:
            used_variables.append((path, subtree.get("value")))

    def historical_table(self, df):
        """This function is made to separate the historical values of a dataframe from the projections

        :param df: dataframe with all the years
        :return: dataframe with historical values (year <= baseyear)
        """
        # FIXME: the flow_vars baseyear is not available by all the nodes. The baseyear was hardcoded in the meantime.
        baseyear = 2021  # int(self.flow_vars["baseyear"])
        if "Years" not in df.columns:
            self.logger.error(
                "The table in the node {} does not contain Years column".format(
                    self.knime_name
                )
            )
            raise Exception
        df_hist = df.copy()
        df_hist["Years"] = df_hist["Years"].astype(int)
        df_hist = df_hist.loc[df_hist["Years"] < baseyear]
        df_hist["Years"] = df_hist["Years"].astype(str)
        return df_hist

    def projections_table(self, df):
        """This function is made to separate the projections values of a dataframe from the historical data

        :param df: dataframe with all the years
        :return: dataframe with projections values (year > baseyear)
        """
        # FIXME: the flow_vars baseyear is not available by all the nodes. The baseyear was hardcoded in the meantime.
        baseyear = 2021  # int(self.flow_vars["baseyear"])
        if "Years" not in df.columns:
            self.logger.error(
                "The table in the node {} does not contain Years column".format(
                    self.knime_name
                )
            )
            return df
        df_proj = df.copy()
        df_proj["Years"] = df_proj["Years"].astype(int)
        df_proj = df_proj.loc[df_proj["Years"] >= baseyear]
        df_proj["Years"] = df_proj["Years"].astype(str)
        return df_proj

    def merge_hist_proj_table(self, df_hist, df_proj):
        """Merge historical and projection dataframes

        :param df_hist: historical dataframe
        :param df_proj: projection dataframe
        :return: merged dataframe
        """
        return pd.concat([df_hist, df_proj])
