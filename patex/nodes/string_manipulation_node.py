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
    String manipulation node
    ========================
    KNIME options implemented:
        - Nothing except
        - indexOf(), the modifiers for replace() are not implemented either
        - substr()
        - string()
        - join()
"""

import re

import pandas as pd

from patex.nodes.node import Context, PythonNode, NativeNode


PATTERN = re.compile("\$(.*?)\$")


class StringManipulationNode(PythonNode, NativeNode):
    knime_name = "String Manipulation"

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

        # Find all column names in the expression thanks to the $...$ syntax:
        for var_string in re.findall(PATTERN, self.expression):
            # Replace all column names in the expression with their expression:
            self.expression = self.expression.replace(f"${var_string}$", f"output['{var_string}']")

        self.expression = "output = " + self.expression

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
            self.logger.error("Error in node " + str(
                self.id) + " : insert missing as null not implemented! Node's xml file is: " + xml_file)

        self = PythonNode.init_wrapper(cls, expression=expression, var_name=var_name)
        self.init_knime(ctx, id, xml_root, xmlns, flow_vars_params)
        return self

    def apply(self, df) -> pd.DataFrame:
        # Copy input to output
        self.ns['output'] = df.copy()

        exec(self.expression, self.ns)

        df[self.var_name] = self.ns['output']

        return df

    def join(self, *arg):
        output_colmun = arg[0]
        for i in range(1, len(arg)):
            output_colmun += arg[i]
        return output_colmun

    def replace(self, str_to_modify, search, replace_with, modifiers=None):
        if modifiers:
            self.logger.error("Error in node " + str(
                self.id) + " : error in expression for string manipulation: replace not implemented! Node's xml file is: " + self.xml_file)
        output_column = str_to_modify.str.replace(search, replace_with)
        return output_column

    def capitalize(self, string_to_modify, chars=None):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: capitalize not implemented! Node's xml file is: " + self.xml_file)

    def compare(self, string_to_modify1, string_to_modify2):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: compare not implemented! Node's xml file is: " + self.xml_file)

    def count(self, string_to_modify, toCount, modifiers=None):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: count not implemented! Node's xml file is: " + self.xml_file)

    def countChars(self, string_to_modify, chars, modifiers=None):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: countChars not implemented! Node's xml file is: " + self.xml_file)

    def indexOf(self, column_to_modify, toSearch, start=None, modifiers=None):
        if modifiers is not None:
            self.logger.error("Error in node " + str(
                self.id) + " : the indexOf() function with modifiers != None is not implemented! Node's xml file is: " + self.xml_file)
        # self.logger.error("Error in node " + str(
        #     self.id) + " : error in expression for string manipulation: indexOf not implemented! Node's xml file is: " + self.xml_file)
        return column_to_modify.str.index(toSearch, start)

    def indexOfChars(self, string_to_modify, toSearch, start=None, modifiers=None):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: indexOfChars not implemented! Node's xml file is: " + self.xml_file)

    def joinSep(self, sep, string_to_modify, *arg):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: joinSep not implemented! Node's xml file is: " + self.xml_file)

    def lastIndexOfChar(self, string_to_modify, char):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: lastIndexOfChar not implemented! Node's xml file is: " + self.xml_file)

    def length(self, string_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: length not implemented! Node's xml file is: " + self.xml_file)

    def lowerCase(self, string_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: lowerCase not implemented! Node's xml file is: " + self.xml_file)

    def regexMatcher(self, string_to_modify, regex):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: regexMatcher not implemented! Node's xml file is: " + self.xml_file)

    def regexReplace(self, string_to_modify, regex, replacestring_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: regexReplace not implemented! Node's xml file is: " + self.xml_file)

    def removeChars(self, string_to_modify, chars=None):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: removeChars not implemented! Node's xml file is: " + self.xml_file)

    def removeDiacritic(self, string_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: removeDiacritic not implemented! Node's xml file is: " + self.xml_file)

    def removeDuplicates(self, string_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: removeDuplicates not implemented! Node's xml file is: " + self.xml_file)

    def replaceChars(self, string_to_modify, chars, replace, modifiers=None):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: replaceChars not implemented! Node's xml file is: " + self.xml_file)

    def replaceUmlauts(self, string_to_modify, omitE):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: replaceUmlauts not implemented! Node's xml file is: " + self.xml_file)

    def reverse(self, string_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: reverse not implemented! Node's xml file is: " + self.xml_file)

    def string(self, x):
        return str(x)

    def strip(self, string_to_modify, *arg):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: strip not implemented! Node's xml file is: " + self.xml_file)

    def stripEnd(self, string_to_modify, *arg):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: stripEnd not implemented! Node's xml file is: " + self.xml_file)

    def substr(self, column_to_modify, start, length=None):
        column = column_to_modify.str[start:]
        if length is not None:
            column = column.to_frame().join(length.rename('length'))
            column = column.apply(lambda x: x.iloc[0][:x.iloc[1]], 1)
        return column

    def toBoolean(self, x):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: toBoolean not implemented! Node's xml file is: " + self.xml_file)

    def toDouble(self, x):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: toDouble not implemented! Node's xml file is: " + self.xml_file)

    def toEmpty(self, string_to_modify, *arg):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: toEmpty not implemented! Node's xml file is: " + self.xml_file)

    def toInt(self, x):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: toInt not implemented! Node's xml file is: " + self.xml_file)

    def toLong(self, x):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: toLong not implemented! Node's xml file is: " + self.xml_file)

    def toNull(self, string_to_modify, *arg):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: toNull not implemented! Node's xml file is: " + self.xml_file)

    def upperCase(self, string_to_modify):
        self.logger.error("Error in node " + str(
            self.id) + " : error in expression for string manipulation: upperCase not implemented! Node's xml file is: " + self.xml_file)


if __name__ == '__main__':
    import pandas as pd

    id = '3855'
    relative_path = f'/Users/climact/XCalc/dev/electricity_supply_prototype/workflows/power_supply_processing/5_0 Power Su (#3826)/TRA to ELC _ (#3853)/String Manipulation (#{id})/settings.xml'
    node_type = 'SubNode'
    knime_workspace = '/Users/climact/XCalc/dev'
    node = StringManipulationNode(id, relative_path, node_type, knime_workspace)
    node.in_ports[1] = pd.read_csv('/Users/climact/Desktop/in.csv')
    node.run()
    node.out_ports[1].to_csv('/Users/climact/Desktop/out_pycharm.csv', index=False)

    pd.testing.assert_frame_equal(pd.read_csv('/Users/climact/Desktop/out_pycharm.csv'),
                                  pd.read_csv('/Users/climact/Desktop/out_knime.csv'))
