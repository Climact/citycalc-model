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

"""
    STRING MANIPULATION VARIABLE NODE
    ============================
    KNIME options implemented:
        - Nothing except join(), string() and replace(), the modifiers for replace() are not implemented either
"""

import re

from patex.nodes.node import Context, PythonNode, NativeNode, FlowVars


PATTERN = re.compile("\$\$\{.*?}\$\$")


class StringManipulationVariableNode(PythonNode, NativeNode):
    knime_name = "String Manipulation (Variable)"

    def __init__(self, expression: str, var_name: str):
        super().__init__()
        self.expression = expression
        self.var_name = var_name

        self.ns = {'output': None, 'join': self.join, 'replace': self.replace, 'capitalize': self.capitalize,
                   'compare': self.compare, 'count': self.count, 'countChars': self.countChars, 'indexOf': self.indexOf,
                   'indexOfChars': self.indexOfChars, 'joinSep': self.joinSep, 'lastIndexOfChar': self.lastIndexOfChar,
                   'length': self.length, 'lowerCase': self.lowerCase, 'regexMatcher': self.regexMatcher,
                   'regexReplace': self.regexReplace, 'removeChars': self.removeChars,
                   'removeDiacritic': self.removeDiacritic,
                   'removeDuplicates': self.removeDuplicates, 'replaceChars': self.replaceChars,
                   'replaceUmlauts': self.replaceUmlauts, 'reverse': self.reverse, 'string': self.string,
                   'strip': self.strip, 'stripEnd': self.stripEnd, 'substr': self.substr, 'toBoolean': self.toBoolean,
                   'toDouble': self.toDouble, 'toEmpty': self.toEmpty, 'toInt': self.toInt, 'toLong': self.toLong,
                   'toNull': self.toNull, 'upperCase': self.upperCase}

    def init_ports(self):
        self.in_ports = {1: None}
        self.out_ports = {1: None}

    @classmethod
    def from_knime(cls, ctx: Context, id, xml_root, xmlns, flow_vars_params):
        model = xml_root.find(xmlns + "config[@key='model']")
        expression = model.find(xmlns + "entry[@key='expression']").get('value')
        var_name = model.find(xmlns + "entry[@key='replaced_column']").get('value')
        insert_missing_as_null = model.find(xmlns + "entry[@key='insert_missing_as_null']").get('value')

        if insert_missing_as_null != "false":
            cls.knime_error(id, "insert missing as null not implemented")

        self = PythonNode.init_wrapper(cls, expression=expression, var_name=var_name)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, flow_vars: "FlowVars") -> "FlowVars":
        # FIXME > to clean
        if getattr(self, "id", None) == '1735':
            flow_vars["disable_production_calibration_string"] = flow_vars["disable_product_calibration"]
        elif getattr(self, "id", None) == "1734":
            flow_vars["disable_production_calibration_string"] = flow_vars["disable_product_calibration"]
        else:

            # Find all variables in the expression thanks to the $${...}$$ syntax:
            for var_string in re.findall(PATTERN, self.expression):
                # Replace all variables in the expression with their values:
                var_value = flow_vars[var_string[4:-3]]
                self.expression = self.expression.replace(var_string, "'" + str(var_value) + "'")

            self.expression = "output = " + self.expression

            exec(self.expression, self.ns)

            if self.var_name in flow_vars:
                raise self.exception(f"Node is trying to overwrite variable{self.var_name}!")
            else:
                flow_vars[self.var_name] = self.ns['output']
        return flow_vars

    def join(*arg):
        return ''.join(arg[1:])

    def replace(self, str_to_modify, search, replace_with, modifiers=None):
        if modifiers is not None:
            self.error("the replace() function with modifiers != None is not implemented")
        return str_to_modify.replace(search, replace_with)

    def capitalize(self, string_to_modify, chars=None):
        self.error("error in expression for string manipulation: capitalize not implemented")

    def compare(self, string_to_modify1, string_to_modify2):
        self.error("error in expression for string manipulation: compare not implemented")

    def count(self, string_to_modify, toCount, modifiers=None):
        self.error("error in expression for string manipulation: count not implemented")

    def countChars(self, string_to_modify, chars, modifiers=None):
        self.error("error in expression for string manipulation: countChars not implemented")

    def indexOf(self, string_to_modify, toSearch, start=None, modifiers=None):
        self.error("error in expression for string manipulation: indexOf not implemented")

    def indexOfChars(self, string_to_modify, toSearch, start=None, modifiers=None):
        self.error("error in expression for string manipulation: indexOfChars not implemented")

    def joinSep(self, sep, string_to_modify, *arg):
        self.error("error in expression for string manipulation: joinSep not implemented")

    def lastIndexOfChar(self, string_to_modify, char):
        self.error("error in expression for string manipulation: lastIndexOfChar not implemented")

    def length(self, string_to_modify):
        self.error("error in expression for string manipulation: length not implemented")

    def lowerCase(self, string_to_modify):
        self.error("error in expression for string manipulation: lowerCase not implemented")

    def regexMatcher(self, string_to_modify, regex):
        self.error("error in expression for string manipulation: regexMatcher not implemented")

    def regexReplace(self, string_to_modify, regex, replacestring_to_modify):
        self.error("error in expression for string manipulation: regexReplace not implemented")

    def removeChars(self, string_to_modify, chars=None):
        self.error("error in expression for string manipulation: removeChars not implemented")

    def removeDiacritic(self, string_to_modify):
        self.error("error in expression for string manipulation: removeDiacritic not implemented")

    def removeDuplicates(self, string_to_modify):
        self.error("error in expression for string manipulation: removeDuplicates not implemented")

    def replaceChars(self, string_to_modify, chars, replace, modifiers=None):
        self.error("error in expression for string manipulation: replaceChars not implemented")

    def replaceUmlauts(self, string_to_modify, omitE):
        self.error("error in expression for string manipulation: replaceUmlauts not implemented")

    def reverse(self, string_to_modify):
        self.error("error in expression for string manipulation: reverse not implemented")

    def string(self, x):
        return str(x)
        # self.error("error in expression for string manipulation: string not implemented")

    def strip(self, string_to_modify, *arg):
        self.error("error in expression for string manipulation: strip not implemented")

    def stripEnd(self, string_to_modify, *arg):
        self.error("error in expression for string manipulation: stripEnd not implemented")

    def substr(self, string_to_modify, start, length=None):
        self.error("error in expression for string manipulation: substr not implemented")

    def toBoolean(self, x):
        self.error("error in expression for string manipulation: toBoolean not implemented")

    def toDouble(self, x):
        return float(x)

    def toEmpty(self, string_to_modify, *arg):
        self.error("error in expression for string manipulation: toEmpty not implemented")

    def toInt(self, x):
        self.error("error in expression for string manipulation: toInt not implemented")

    def toLong(self, x):
        self.error("error in expression for string manipulation: toLong not implemented")

    def toNull(self, string_to_modify, *arg):
        self.error("error in expression for string manipulation: toNull not implemented")

    def upperCase(self, string_to_modify):
        self.error("error in expression for string manipulation: upperCase not implemented")
