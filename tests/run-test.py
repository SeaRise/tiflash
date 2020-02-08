# -*- coding:utf-8 -*-

import os
import sys
import time

CMD_PREFIX = '>> '
CMD_PREFIX_ALTER = '=> '
CMD_PREFIX_TIDB = 'mysql> '
RETURN_PREFIX = '#RETURN'
SLEEP_PREFIX = 'SLEEP '
TODO_PREFIX = '#TODO'
COMMENT_PREFIX = '#'
UNFINISHED_1_PREFIX = '\t'
UNFINISHED_2_PREFIX = '   '
WORD_PH = '{#WORD}'

class Executor:
    def __init__(self, dbc):
        self.dbc = dbc
    def exe(self, cmd):
        return os.popen((self.dbc + ' "' + cmd + '" 2>&1').strip()).readlines()

def parse_line(line):
    words = [w.strip() for w in line.split("│") if w.strip() != ""]
    return "@".join(words)

def parse_table_parts(lines, fuzz):
    parts = set()
    if not fuzz:
        curr = []
        for line in lines:
            if line.startswith('┌'):
                if len(curr) != 0:
                    parts.add('\n'.join(curr))
                    curr = []
            curr.append(parse_line(line))
        if len(curr) != 0:
            parts.add('\n'.join(curr))
    else:
        for line in lines:
            if not line.startswith('┌') and not line.startswith('└'):
                line = parse_line(line)
                if line in parts:
                    line += '-extra'
                parts.add(line)
    return parts

def is_blank_char(c):
    return c in [' ', '\n', '\t']

def is_brace_char(c):
    return c in ['{', '[', '(', ')', ']', '}']

def is_break_char(c):
    return (c in [',', ';']) or is_brace_char(c) or is_blank_char(c)

def match_ph_word(line):
    i = 0
    while is_blank_char(line[i]):
        i += 1
    found = False
    while not is_break_char(line[i]):
        i += 1
        found = True
    if not found:
        return 0
    return i

# TODO: Support more place holders, eg: {#NUMBER}
def compare_line(line, template):
    while True:
        i = template.find(WORD_PH)
        if i < 0:
            return line == template
        else:
            if line[:i] != template[:i]:
                return False
            j = match_ph_word(line[i:])
            if j == 0:
                return False
            template = template[i + len(WORD_PH):]
            line = line[i + j:]

class MySQLCompare:
    @staticmethod
    def parse_output_line(line):
        words = [w.strip() for w in line.split("\t") if w.strip() != ""]
        return "@".join(words)
    @staticmethod
    def parse_mysql_line(line):
        words = [w.strip() for w in line.split("|") if w.strip() != ""]
        return "@".join(words)
    @staticmethod
    def parse_mysql_outputs(outputs):
        results = set()
        for output_line in outputs:
            parsed_line = MySQLCompare.parse_output_line(output_line)
            while parsed_line in results:
                parsed_line += '-extra'
            results.add(parsed_line)
        return results
    @staticmethod
    def parse_excepted_outputs(outputs):
        results = set()
        for output_line in outputs:
            if not output_line.startswith('+'):
                parsed_line = MySQLCompare.parse_mysql_line(output_line)
                while parsed_line in results:
                    parsed_line += '-extra'
                results.add(parsed_line)
        return results
    @staticmethod
    def matched(outputs, matches):
        if len(outputs) == 0 and len(matches) == 0:
            return True
        is_table_parts = len(matches) > 0 and matches[0].startswith('+')
        if is_table_parts:
            a = MySQLCompare.parse_mysql_outputs(outputs)
            b = MySQLCompare.parse_excepted_outputs(matches)
            return a == b
        else:
            if len(outputs) != len(matches):
                return False
            for i in range(0, len(outputs)):
                if not compare_line(outputs[i], matches[i]):
                    return False
            return True

def matched(outputs, matches, fuzz):
    if len(outputs) == 0 and len(matches) == 0:
        return True

    is_table_parts = len(matches) > 0 and matches[0].startswith('┌')
    if is_table_parts:
        a = parse_table_parts(outputs, fuzz)
        b = parse_table_parts(matches, fuzz)
        return a == b
    else:
        if len(outputs) != len(matches):
            return False
        for i in range(0, len(outputs)):
            if not compare_line(outputs[i], matches[i]):
                return False
        return True

class Matcher:
    def __init__(self, executor, executor_tidb, fuzz):
        self.executor = executor
        self.executor_tidb = executor_tidb
        self.fuzz = fuzz
        self.query = None
        self.outputs = None
        self.matches = []
        self.is_mysql = False

    def on_line(self, line):
        if line.startswith(SLEEP_PREFIX):
            time.sleep(float(line[len(SLEEP_PREFIX):]))
        elif line.startswith(CMD_PREFIX_TIDB):
            if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
                return False
            self.is_mysql = True 
            self.query = line[len(CMD_PREFIX_TIDB):]
            self.outputs = self.executor_tidb.exe(self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.matches = []
        elif line.startswith(CMD_PREFIX) or line.startswith(CMD_PREFIX_ALTER):
            if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
                return False
            self.is_mysql = False
            self.query = line[len(CMD_PREFIX):]
            self.outputs = self.executor.exe(self.query)
            self.outputs = map(lambda x: x.strip(), self.outputs)
            self.outputs = filter(lambda x: len(x) != 0, self.outputs)
            self.matches = []
        else:
            self.matches.append(line)
        return True

    def on_finish(self):
        if self.outputs != None and not matched(self.outputs, self.matches, self.fuzz):
            return False
        return True

def parse_exe_match(path, executor, executor_tidb, fuzz):
    todos = []
    with open(path) as file:
        matcher = Matcher(executor, executor_tidb, fuzz)
        cached = None
        for origin in file:
            line = origin.strip()
            if line.startswith(RETURN_PREFIX):
                break
            if line.startswith(TODO_PREFIX):
                todos.append(line[len(TODO_PREFIX):].strip())
                continue
            if line.startswith(COMMENT_PREFIX) or len(line) == 0:
                continue
            if origin.startswith(UNFINISHED_1_PREFIX) or origin.startswith(UNFINISHED_2_PREFIX):
                if cached[-1] == ',':
                    cached += ' '
                cached += line
                continue
            if cached != None and not matcher.on_line(cached):
                return False, matcher, todos
            cached = line
        if (cached != None and not matcher.on_line(cached)) or not matcher.on_finish():
            return False, matcher, todos
        return True, matcher, todos

def run():
    if len(sys.argv) != 5:
        print 'usage: <bin> tiflash-client-cmd test-file-path fuzz-check tidb-client-cmd'
        sys.exit(1)

    dbc = sys.argv[1]
    path = sys.argv[2]
    fuzz = (sys.argv[3] == 'true')
    mysql_client = sys.argv[4]

    matched, matcher, todos = parse_exe_match(path, Executor(dbc), Executor(mysql_client), fuzz)

    def display(lines):
        if len(lines) == 0:
            print ' ' * 4 + '<nothing>'
        else:
            for it in lines:
                print ' ' * 4 + it

    if not matched:
        print '  File:', path
        print '  Error:', matcher.query
        print '  Result:'
        display(matcher.outputs)
        print '  Expected:'
        display(matcher.matches)
        sys.exit(1)
    if len(todos) != 0:
        print '  TODO:'
        for it in todos:
            print ' ' * 4 + it

def main():
    try:
        run()
    except KeyboardInterrupt:
        print 'KeyboardInterrupted'
        sys.exit(1)

main()
