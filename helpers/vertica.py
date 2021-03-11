import pandas as pd
import numpy as np
import datetime as dt
import string
import ast
import re

import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
    
def create_cases(col1: str, col2: str, data):
    '''
    Function for create structures "case when ... then ... " from DataFrame of versions of the formula
    Parameters:
        col1 - name of column with name of the counter
        col2 - name of column with formula for the counter
        data - DataFrame with all versions of the formula from VERTICA_STAT.FORMULA or VERTICA_STAT.NUM_DENOM
    '''
    # final counters lives here
    counters = {}
    
    # for every unique first level KPI
    for kpi_short in list(set(data[col1])):

        # if there was only one version of the formula
        if len(data.loc[data[col1] == kpi_short]) == 1:
            #print(data.loc[data[col1] == kpi_short][col1])
            counters[kpi_short] = list(data.loc[data[col1] == kpi_short][col2])[0] # just adding it to the dict

        # if the were some changes    
        else:
            # getting all versions of the 1st lvl formula
            all_fixes = data.loc[data[col1] == kpi_short] 
            # cast dates into needed format (get rid of hours)
            all_fixes['DATE_START'] = [i.date() for i in all_fixes['DATE_START'] ]
            # sorting (IMPORTANT) to have normal "where" later
            all_fixes = all_fixes.sort_values(by = ['DATE_START'], ascending=False)
            case = {}
            case['case'] = {}
            case['case']['when'] = []
            # previous data (for not getting date of end though some tricks)
            prev_value = dt.datetime.today().date() + dt.timedelta(days=2)

            # for every change of formulas (sorted)
            for fix in all_fixes.iterrows():
                
                # if we know the end_date (so always, except of the newest formula)
                if prev_value:
                    # adding "between"
                    t = {'between': {'counter':'datetime',
                                     'date_start': fix[1]['DATE_START'],
                                     'date_end': prev_value - dt.timedelta(days=1),
                                     'by_date': True }}
                    case['case']['when'].append([t, f"{fix[1][col2]}"])
                    # update end_date
                    prev_value = fix[1]['DATE_START']

            case['case']["else"] = 'Null'
            # init counter this monstrous "case"
            counters[kpi_short] = case  
            
    return counters


def prepare_formula(formula: str):
    '''
    Function for small enhancements for the formula.
    It's in the separate function so if any other operations on the formula should be done,
        make it more uniformal.
    '''
    fixed = formula.replace(' ', '')#.replace('.', '_')
    # Happend a lot when formala looked like (a+b)*c = a*c + b*c etc., so I added this for taking only the last one
    fixed = fixed.split('=')
    return fixed[-1]


def add_aggr_nvl(formula: str, agr = 'SUM'):
    '''
    Function that wrap all counters into SUM(NVL(..., 0)) (or not SUM, depends on the input)
    Parameters:
        formula 
        agr - aggregation method (SUM, AVG, MIN, MAX, etc) 
    '''
    def recurse(node: ast.Expression, temp: list):
        '''
        Local function for recursion over all leaves in the tree.
        Parameters:
            node - current node and all its children
            temp - array where the answer lives
        '''
        # if the expr is Binary Operation
        if isinstance(node, ast.BinOp):
            # and if it's + - * or /
            if isinstance(node.op, ast.Add) or isinstance(node.op, ast.Sub) or isinstance(node.op, ast.Div) or isinstance(node.op, ast.Mult):
                # adding open parentheses
                temp.append(['('])
            # go to the left part of expr
            recurse(node.left, temp)
            # go to the operation of the expr
            recurse(node.op, temp)
            # go to the right part of expr
            recurse(node.right, temp)
            if isinstance(node.op, ast.Div) or isinstance(node.op, ast.Mult) or isinstance(node.op, ast.Add) or isinstance(node.op, ast.Sub):
                # adding close parentheses
                temp.append([')'])
        # if it's operation +
        elif isinstance(node, ast.Add):
            temp.append(['+'])
        # if it's operation -
        elif isinstance(node, ast.Sub):
            temp.append(['-'])
        # if it's operation *
        elif isinstance(node, ast.Mult):
            temp.append(['*'])
        # if it's operation /
        elif isinstance(node, ast.Div):
            temp.append(['/'])
        # if it's number
        elif isinstance(node, ast.Num):
            temp.append([str(node.n)])
        # if it's counter ( none number )    
        elif isinstance(node, ast.Name):
            temp.append(['{}(NVL('.format(agr)+node.id+', 0))'])
        # is other cases going though all children of the expresion
        else:
            for child in ast.iter_child_nodes(node):
                recurse(child, temp)
                return temp
    # creating a placeholder for the answer
    temp = []  
    # parse the string formula
    formula = ast.parse(prepare_formula(formula), mode='eval')
    if formula is not None:
        recurse(formula, temp)
    return ''.join([i[0] for i in temp])


def check_tree_for_instance(formula: str, instance: type):
    '''
    Function to check if any of the children of the expresion is instance of given structure.
    The logic is absolutely the same as in the previous function. Feel free to consult it if needed.
    Parameters:
        formula - formula
            'a + b - c * 16'
        instance - instance (one of AST's)
            ast.Mult
    '''
    def recurse(node: ast.Expression, temp: list):
        '''
        Recursion for going though every children
        Parameters:
            node - node
            temp - placeholder for answer
        '''
        # if the expr is Binary Operation
        if isinstance(node, ast.BinOp):
            # and is instance
            if isinstance(node.op, instance):
                # adding True to the answer
                temp[0] = True
            # go to the left part of expr
            recurse(node.left, temp)
            # go to the operation of the expr
            recurse(node.op, temp)
            # go to the right part of expr
            recurse(node.right, temp)
           
        # if not BinOp but instance of needed class 
        elif isinstance(node, instance):
            temp[0] = True
        else:
            for child in ast.iter_child_nodes(node):
                recurse(child, temp)
                return temp
            
    formula = ast.parse(prepare_formula(formula), mode='eval')
    temp = [False]
   
    if formula is not None:
        recurse(formula, temp)
    return temp[0]


def check_intervals(st, en, cases):
    '''
    Function for checking if 2 time intervals intersect. 
    Is used for checking if every version of formula was used in any given time frame.
    Parameters:
        st - start date of time interval that needs to be checked
        en - end date of time interval that needs to be checked
        cases - list of dicts with all of the formula changes
    '''
    text  = 'case'
    # for every formula change
    for case in cases:
        # saving start and end dates of the change into separate variables
        t1start = case[0]['between']['date_start']
        t1end = case[0]['between']['date_end']
        # if time interval started while this formula version was used
        if (t1start <= st <= t1end):
            # adding the formula usage from beginning of time interval to the another formula version
            text+=' when ' + case[0]['between']['counter']
            text+=f" between to_date('{st.date().isoformat()}', 'YYYY-MM-DD') and to_date('{t1end.isoformat()}', 'YYYY-MM-DD')" 
            text+=f" then {case[1]} "
        # if formula stoped being used while time interval
        elif (st <= t1start <= en):
            # adding the formula usage from beginning of this formula version until the end of time interval
            text+=' when ' + case[0]['between']['counter']
            text+=f" between to_date('{t1start.isoformat()}', 'YYYY-MM-DD') and to_date('{en.isoformat()}', 'YYYY-MM-DD')" 
            text+=f" then {case[1]}"
    text+=" else null end"
    return text


def check_tree_for_replace_dividers(formula: str, dt_s, dt_e, dictionary: dict):
    '''
    Function for finding all counters in first level formulas and replacing them with version of second level formulas with cases.
    The logic is absolutely the same as in the previous 2 functions of this kind. Feel free to consult them if needed.
    Parameters:
        formula
        dt_s - date of start of this version of the 1st lvl formula
        dt_e - date of end of this version of the 1st lvl formula
        dictionary - dict with cases for secons level formulas
    '''
    def recurse(node: ast.Expression, temp: list):
        '''
        Recursion for going though every children adding ONLINESTAT.F_DEV
        Parameters:
            node - node
            temp - placeholder for answer
        '''
        # if the expr is Binary Operation 
        if isinstance(node, ast.BinOp):
            # if it's + - or * then just adding parentheses and going recursivly inside children
            if isinstance(node.op, ast.Add) or isinstance(node.op, ast.Sub) or isinstance(node.op, ast.Mult):
                temp.append(['('])
                recurse(node.left, temp)
                recurse(node.op, temp)
                recurse(node.right, temp)
                
            # if it's division    
            elif isinstance(node.op, ast.Div):
                # if 5/COUNTERS -> ONLINESTAT.F_DEV
                if isinstance(node.left, ast.Num) and not isinstance(node.right, ast.Num):
                    temp.append(['ONLINESTAT.F_DEV('])
                    temp.append([str(node.left.n)])
                    # here adding "," not /         !!!!
                    temp.append([', '])
                    recurse(node.right, temp)
                # if COUNTER1/COUNTER2 -> ONLINESTAT.F_DEV    
                elif not isinstance(node.right, ast.Num) and not isinstance(node.left, ast.Num):
                    temp.append(['ONLINESTAT.F_DEV('])
                    recurse(node.left, temp)
                    # here adding "," not /         !!!!
                    temp.append([', '])
                    recurse(node.right, temp)
                # if 5/10 or COUNTER/5 -> simple division    
                else:
                    recurse(node.left, temp)
                    recurse(node.op, temp)
                    recurse(node.right, temp)
            # closing parentheses (same for everithing)
            if isinstance(node.op, ast.Div) or isinstance(node.op, ast.Mult) or isinstance(node.op, ast.Add) or isinstance(node.op, ast.Sub):
                temp.append([')'])
        elif isinstance(node, ast.Add):
            temp.append(['+'])
        elif isinstance(node, ast.Sub):
            temp.append(['-'])
        elif isinstance(node, ast.Mult):
            temp.append(['*'])
        elif isinstance(node, ast.Div):
            temp.append(['/'])
        elif isinstance(node, ast.Num):
            temp.append([str(node.n)])
        elif isinstance(node, ast.Name):
            if dictionary.get(node.id):
                # if there are some versions of this formula in dict adding to the answer
                if isinstance(dictionary.get(node.id), dict):
                    temp.append([check_intervals(dt_s, dt_e, dictionary.get(node.id).get('case').get('when'))])
                else:
                    temp.append([dictionary.get(node.id, node.id)])
            else:
                temp.append([node.id])
        else:
            for child in ast.iter_child_nodes(node):
                recurse(child, temp)
                return temp
            
    temp = []   
    formula = ast.parse(prepare_formula(formula), mode='eval')
    
    if formula is not None:
        recurse(formula, temp)
        
    return ''.join([i[0] for i in temp])


def get_all_counters(formula):
    '''
    Function that returns all counters present in the formula.
    It's useful and more convenient than regex becauses it's almost 100% accurate.
    Parameters:
        formula 
    '''
    def recurse(node: ast.Expression, temp: list):
        '''
        Local function for recursion over all leaves in the tree.
        Parameters:
            node - current node and all its children
            temp - array where the answer lives
        '''
        if isinstance(node, ast.BinOp):
            # go to the left part of expr
            recurse(node.left, temp)
            # go to the operation of the expr
            recurse(node.op, temp)
            # go to the right part of expr
            recurse(node.right, temp)
        # if it's the counter
        if isinstance(node, ast.Name):
            temp.append([node.id])
        # is other cases going though all children of the expresion
        else:
            for child in ast.iter_child_nodes(node):
                recurse(child, temp)
                return temp
            
    # creating a placeholder for the answer
    temp = []  
    # parse the string formula
    formula = ast.parse(prepare_formula(formula), mode='eval')
    if formula is not None:
        recurse(formula, temp)
        
    return list(set([i[0] for i in temp]))



#----------------------obsolete------------------------------------




def create_cases_str(col, col1, col2, data):
    # final counters lives here
    counters = {}
    # for every unique first level KPI
    for kpi_short in list(set(data[col1])):

        # if there was only one version of the formula
        if len(data.loc[data[col1] == kpi_short]) == 1:
            counters[kpi_short] = line[1][col] # just adding it to the dict

        # if the were some changes    
        else:
            # getting all versions of the 1st lvl formula
            all_fixes = data.loc[data[col1] == kpi_short] 
            # cast dates into needed format (get rid of hours)
            all_fixes['DATE_START'] = [i.date() for i in all_fixes['DATE_START'] ]
            # sorting (IMPORTANT) to have normal "where" later
            all_fixes = all_fixes.sort_values(by = ['DATE_START'], ascending=False)
            case = 'nvl(case \n'

            # previous data (for not getting date of end though some tricks)
            prev_value = None

            # for every change of formulas (sorted)
            for fix in all_fixes.iterrows():

                # if we know the end_date (so always, except of the newest formula)
                if prev_value:
                    # adding "between"
                    case += (f"when datetime between to_date({fix[1]['DATE_START'].isoformat()}, 'YYYY-MM-DD') \
 and to_date({prev_value}, 'YYYY-MM-DD')-1 then ({fix[1][col2]})\n")
                    # update end_date
                    prev_value = fix[1]['DATE_START'].isoformat() 

                # if it's the newest formula
                else:
                    # adding there everything later than date
                    case += (f"when datetime >= to_date({fix[1]['DATE_START'].isoformat()}, 'YYYY-MM-DD') \
 then ({fix[1][col]})\n")
                    # init end_date
                    prev_value = fix[1]['DATE_START'].isoformat() 

            case+=("else Null end , 0)")
            # init counter this monstrous "case"
            counters[kpi_short] = case  
            
    return counters