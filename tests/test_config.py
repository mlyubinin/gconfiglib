import nose
from nose.tools import raises
import os
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import gconfiglib as cfg
from gconfiglib import config
from gconfiglib import utils

# used for immutability test
global_val = 5

def setup_module():
    #config.init('tests/import.conf_test')
    pass


# read_config

def test_read_config_no_file():
    with nose.tools.assert_raises(Exception) as e:
        config.read_config('no_file')
    nose.tools.ok_(str(e.exception) == 'File no_file does not exist or is not readable', 'Non-existent filename should raise an exception')

def test_read_config_no_rights():
    os.system('chmod a-r tests/test_utils_no_rights')
    if os.access('tests/test_utils_no_rights', os.R_OK):
        # running inside protected environment, cannot do this test
        assert True
    else:
        with nose.tools.assert_raises(Exception) as e:
            config.read_config('tests/test_utils_no_rights')
        os.system('chmod a+r tests/test_utils_no_rights')
        print e.exception
        nose.tools.ok_(str(e.exception) == 'File tests/test_utils_no_rights does not exist or is not readable', 'File without read permissions should raise an exception')

def test_read_config_empty():
    with nose.tools.assert_raises(Exception) as e:
        config.read_config('tests/test_utils_empty')
    nose.tools.ok_(str(e.exception) == 'Empty configuration file tests/test_utils_empty', 'Empty config file should raise an exception')

def test_read_config_no_separator():
    res = config.read_config('tests/test_utils_cases').get('no_separator', None)
    print res
    assert res is None

def test_read_config_no_value():
    assert config.read_config('tests/test_utils_cases').get('no_value', None) is None

def test_read_config_double_sign():
    assert config.read_config('tests/test_utils_cases').get('double_sign', None) == 'value ='

def test_read_config_positive():
    assert config.read_config('tests/test_utils_cases').get('key', None) == 'value'

def test_read_config_section_empty():
    assert config.read_config('tests/test_utils_cases').get('empty_section', None) == {}

def test_read_config_section_ok():
    assert config.read_config('tests/test_utils_cases').get('section_1', None).get('key', None) == 'value'

def test_read_config_list_ok():
    lst = config.read_config('tests/test_utils_cases').get('section_1', None).get('list', None)
    print lst
    if len(lst) == 3:
        if lst[0] == 'value1' and lst[1] == '2' and lst[2] == 'value3':
            assert True
        else:
            assert False
    else:
        assert False

def test_read_config_comments():
# There is a set number of values in the cases file. The rest are negative scenarios and comments
    assert len(config.read_config('tests/test_utils_cases'))==4

def test_parse_config_line_empty_section():
    assert config.parse_config_line('[ ]') == (0, '')


def test_initialize_zk():
    cfg.init('tests/import.conf_test')
    if not config._zk_conn:
        config._zk_conn = utils.zk_connect('zookeeper://test:test@zookeeper:2181/')
    cfg.root().write().zk(path='/gconfiglib/test_config', force=True)
    config._cfg_root = None
    cfg.init('zookeeper://test:test@zookeeper:2181/gconfiglib/test_config')
    assert cfg.get('/target/database') == 'test'


def test_zk_hierarchy():
    config._cfg_root = None

    def create_config_template(node):
        template = cfg.TemplateNodeFixed('root', optional=False)
        n1 = cfg.TemplateNodeFixed('n1')
        n11 = cfg.TemplateNodeFixed('n11')
        n12 = cfg.TemplateNodeFixed('n12')
        n111 = cfg.TemplateNodeFixed('n111', node_type='CN')
        n111.add(cfg.TemplateAttributeFixed('attr', value_type=int))
        n11.add(n111)
        n112 = cfg.TemplateNodeFixed('n112')
        n112.add(cfg.TemplateAttributeFixed('attr', value_type=int))
        n11.add(n112)
        n11.add(cfg.TemplateAttributeFixed('n11_attr', value_type=int))
        n121 = cfg.TemplateNodeFixed('n121')
        n121.add(cfg.TemplateAttributeFixed('attr', value_type=int))
        n12.add(n121)
        n122 = cfg.TemplateNodeFixed('n122')
        n122.add(cfg.TemplateAttributeFixed('attr', value_type=int))
        n12.add(n122)
        n1.add(n11)
        n1.add(n12)
        template.add(n1)
        return template
    cfg.init('tests/zk_hierarchy.json', template_gen=create_config_template)
    cfg.root().print_fmt()
    cfg.root().write().zk(path='/gconfiglib/test_zk_hierarchy', force=True)
    config._cfg_root = None
    cfg.init('zookeeper://test:test@zookeeper:2181/gconfiglib/test_zk_hierarchy', template_gen=create_config_template)
    cfg.root().print_fmt()
    cfg.root().write().zk(path='/gconfiglib/test_zk_hierarchy', force=True)
    config._cfg_root = None
    cfg.init('zookeeper://test:test@zookeeper:2181/gconfiglib/test_zk_hierarchy')
    cfg.root().print_fmt()
    r = []
    r.append(cfg.root().node_type == 'AN')
    r.append(cfg.root()._get_obj('n1').node_type == 'AN')
    r.append(cfg.root()._get_obj('n1/n12').node_type == 'CN')
    r.append(cfg.root()._get_obj('n1/n11/n111').node_type == 'CN')
    r.append(cfg.root().get('n1/n11/n11_attr') == 1)
    print r
    config._cfg_root = None
    cfg.init('tests/import.conf_test')
    assert sum(r) == 5


def test_initialize_wrong_file():
    os.system('echo "abc" > test_cfg.pkl')
    with nose.tools.assert_raises(Exception) as e:
        cfg.init('test_cfg.pkl')
    if os.path.isfile('test_cfg.pkl'):
        os.system('rm -f test_cfg.pkl')
    nose.tools.ok_(str(e.exception) == 'Could not read configuration file test_cfg.pkl')
    cfg.init('tests/import.conf_test')


def test_check_template_invalid_template_node_no_content():
    template = cfg.TemplateNodeFixed('empty_node')
    with nose.tools.assert_raises_regexp(ValueError, 'Template for node empty_node has no attributes') as e:
        template.validate(config.ConfigNode('empty_node'))


def test_check_template_invalid_template_duplicate_attribute():
    template = cfg.TemplateNodeFixed('some_node')
    template.add(cfg.TemplateAttributeFixed('attr1'))
    template.add(cfg.TemplateAttributeFixed('attr2'))
    with nose.tools.assert_raises_regexp(ValueError, 'Attribute or node attr1 can only be added to node some_node once') as e:
        template.add(config.TemplateAttributeFixed('attr1'))


def test_check_template_invalid_template_invalid_attribute():
    template = cfg.TemplateNodeFixed('some_node')
    template.add(cfg.TemplateAttributeFixed('attr1'))
    template.add(cfg.TemplateAttributeFixed('attr2'))
    with nose.tools.assert_raises_regexp(ValueError, 'Attempt to add invalid attribute type to some_node template') as e:
        template.add({'a': 3})


def test_check_template_template_ok():
    template = cfg.TemplateNodeFixed('node')
    template.add(cfg.TemplateAttributeFixed('attr1', optional=False, value_type=int))
    subnode = cfg.TemplateNodeFixed('subnode')
    subnode.add(cfg.TemplateAttributeFixed('subattr'))
    template.add(subnode)


def test_check_template_varnode_wrong_attr():
    with nose.tools.assert_raises_regexp(ValueError,
                                         'Attempt to add invalid attribute type to varnode template. This node can contain only one TemplateAttributeVariable attribute template and nothing else') as e:
        cfg.TemplateNodeVariableAttr('varnode', cfg.TemplateAttributeFixed('fixedattr'))


def test_validate_missing_mandatory_attr():
    template = cfg.TemplateNodeFixed('node1')
    template.add(cfg.TemplateAttributeFixed('attr', optional=False))
    with nose.tools.assert_raises_regexp(ValueError, 'Mandatory parameter /node1/attr has not been set, and has no default value') as e:
        template.validate(cfg.ConfigNode('node1'))


def test_validate_section_missing():
    template = cfg.TemplateNodeFixed('root', optional=False)
    no_such_section = cfg.TemplateNodeFixed('no_such_section', optional=False)
    no_such_section.add(cfg.TemplateAttributeFixed('attr'))
    template.add(no_such_section)
    with nose.tools.assert_raises_regexp(ValueError, 'Mandatory node /no_such_section is missing, with no defaults set') as e:
        template.validate(cfg.root())


def test_validate_optional_section_missing():
    template = cfg.TemplateNodeFixed('root', optional=False)
    no_such_section = cfg.TemplateNodeFixed('no_such_section')
    no_such_section.add(cfg.TemplateAttributeFixed('attr'))
    template.add(no_such_section)
    assert template.validate(cfg.root())


def test_validate_type_mismatch_element():
    template = cfg.TemplateNodeFixed('root', optional = False)
    general = cfg.TemplateNodeFixed('general', optional = False)
    general.add(cfg.TemplateAttributeFixed('log_level', value_type = int))
    template.add(general)
    with nose.tools.assert_raises_regexp(ValueError, "Expecting /general/log_level to be of type <type 'int'>") as e:
        template.validate(cfg.root())


def test_validate_type_mismatch_node_as_element():
    template = cfg.TemplateNodeFixed('root', optional = False)
    general = cfg.TemplateNodeFixed('general', optional = False)
    log_level = cfg.TemplateNodeFixed('log_level', optional = False)
    log_level.add(cfg.TemplateAttributeFixed('attr', optional = False, value_type = int))
    general.add(log_level)
    template.add(general)
    with nose.tools.assert_raises_regexp(ValueError, "Configuration object passed for validation to template log_level is not a ConfigNode") as e:
        template.validate(cfg.root())


def test_validate_type_mismatch_element_as_node():
    template = cfg.TemplateNodeFixed('root', optional = False)
    template.add(cfg.TemplateAttributeFixed('general', optional = False, value_type = int))
    with nose.tools.assert_raises_regexp(AttributeError, "'ConfigNode' object has no attribute 'value'") as e:
        template.validate(cfg.root())


def test_validate_fail_value_check():
    template = cfg.TemplateNodeFixed('root', optional = False)
    general = cfg.TemplateNodeFixed('general', optional = False)
    general.add(cfg.TemplateAttributeFixed('log_level', optional = True, value_type = str,
                                         validator = lambda x: x.upper() in ['INFO', 'WARNING', 'ERROR', 'CRITICAL']))
    template.add(general)
    with nose.tools.assert_raises_regexp(ValueError, 'Parameter /general/log_level failed validation for value debug') as e:
        template.validate(cfg.root())


def test_validate_varnode_validator_fail():
    template = cfg.TemplateNodeVariableAttr('varnode',
                                               cfg.TemplateAttributeVariable(validator = lambda x: x in ['YES', 'NO']))
    with nose.tools.assert_raises_regexp(ValueError, 'Parameter /varnode/attr2 failed validation for value yes') as e:
        template.validate(cfg.ConfigNode('varnode', attributes={'attr1': 'YES', 'attr2': 'yes'}))


def test_validate_varnode_empty():
    template = cfg.TemplateNodeVariableAttr('varnode',
                                            cfg.TemplateAttributeVariable(validator = lambda x: x in ['YES', 'NO']),
                                            optional = False)
    with nose.tools.assert_raises_regexp(ValueError, 'Node /varnode cannot be empty') as e:
        template.validate(cfg.ConfigNode('varnode'))


def test_validate_nodeset_missing_node():
    template = cfg.TemplateNodeFixed('root', optional = False)
    source_spec = cfg.TemplateNodeFixed('source_spec', optional = False)
    source_spec.add(cfg.TemplateAttributeFixed('method', optional = False,
                                                  validator = lambda x: x in ['local', 'ftp', 'sftp', 'http', 's3']))
    template.add(cfg.TemplateNodeSet('source_spec', source_spec, ['test_missing_method']))
    with nose.tools.assert_raises_regexp(ValueError, 'Parameter /test_missing_method/method failed validation') as e:
        template.validate(cfg.root())


def test_validate_nodeset_missing_mandatory_node_default_attr():
    # validation for a missing mandatory node in  a nodeset, where all mandatory attributes have default values, should pass
    template = cfg.TemplateNodeFixed('root', optional = False)
    source_spec = cfg.TemplateNodeFixed('source_spec', optional = False)
    source_spec.add(cfg.TemplateAttributeFixed('method', optional = False, default_value='abc'))
    template.add(cfg.TemplateNodeSet('source_spec', source_spec, ['test_missing_node']))
    res = template.validate(cfg.root())
    res.print_fmt()
    assert res.get('/test_missing_node/method') == 'abc'


def test_validate_optional_node_mandatory_attr():
    # validation for optional node with mandatory attributes without default value should pass
    template = cfg.TemplateNodeFixed('root', optional=False)
    opt_node = cfg.TemplateNodeFixed('opt_node', optional=True)
    opt_node.add(cfg.TemplateAttributeFixed('mand_attr', optional=False))
    template.add(opt_node)
    cfg_node = cfg.ConfigNode('root', node_type='CN')
    cfg_node.add(cfg.ConfigNode('some_node', attributes={'attr1': 'val1'}))
    res = template.validate(cfg_node)
    res.print_fmt()
    assert res.list_nodes() == ['some_node']

def test_validate_json_value_type_ok():
    # validation for attribute of json value type
    template = cfg.TemplateNodeFixed('root', optional=False)
    json_node = cfg.TemplateNodeFixed('json_node', optional=False)
    json_node.add(cfg.TemplateAttributeFixed('json_attr', optional=False, value_type=str,
                                                validator=lambda x: json.loads(x)['a']==1))
    template.add(json_node)
    cfg_node = cfg.ConfigNode('root', node_type='CN')
    cfg_node.add(cfg.ConfigNode('json_node', attributes={'json_attr': json.dumps({'a':1, 'b':2})}))
    cfg_node.print_fmt()
    res = template.validate(cfg_node)
    res.print_fmt()
    assert json.loads(res.get('/json_node/json_attr'))['b'] == 2

def test_validate_ok():
    template = cfg.TemplateNodeFixed('root', optional = False)
    # [general]
    def general_validator(spec):
        if 'post_type' in spec.keys():
            if (spec['post_type'].upper() == 'SQL' and 'post_sql' not in spec.keys())\
                or (spec['post_type'].upper() == 'SCRIPT' and 'post_script' not in spec.keys()):
                print 'Missing correct post processing attribute in node general'
                return False
        return True

    general = cfg.TemplateNodeFixed('general', optional = False, validator = general_validator)
    general.add(cfg.TemplateAttributeFixed('log_level',
                                         validator = lambda x: x.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
    general.add(cfg.TemplateAttributeFixed('log_file', optional = False))
    general.add(cfg.TemplateAttributeFixed('log_days_to_keep', optional = False, value_type = int, default_value = 30))
    general.add(cfg.TemplateAttributeFixed('no_update', optional = False,
                                         validator = lambda x: x.upper() in ['YES', 'NO']))
    general.add(cfg.TemplateAttributeFixed('file_archive', optional = False))
    general.add(cfg.TemplateAttributeFixed('post_type', validator = lambda x: x.upper() in ['SQL', 'SCRIPT']))
    general.add(cfg.TemplateAttributeFixed('post_sql'))
    general.add(cfg.TemplateAttributeFixed('post_script'))
    template.add(general)

    # [target]
    target = cfg.TemplateNodeFixed('target', optional = False)
    target.add(cfg.TemplateAttributeFixed('db_server', optional = False))
    target.add(cfg.TemplateAttributeFixed('database', optional = False))
    target.add(cfg.TemplateAttributeFixed('schema', optional = False))
    template.add(target)

    # [zookeeper]
    zookeeper = cfg.TemplateNodeFixed('zookeeper', optional = False)
    zookeeper.add(cfg.TemplateAttributeFixed('zk_host', optional = False))
    template.add(zookeeper)

    # [sources]
    sources = cfg.TemplateNodeVariableAttr('sources',
                                              cfg.TemplateAttributeVariable(validator = lambda x: x.upper() in ['YES', 'NO']),
                                              optional = False)
    template.add(sources)

    # Source spec
    def source_spec_validator(spec):
        if spec['method'] == 'local' and 'file_dir' not in spec.keys():
            print 'Missing mandatory file_dir attribute'
            return False
        elif spec['method'] in ['ftp', 'sftp', 'http', 's3'] and 'url' not in spec.keys():
            print 'Missing mandatory url attribute'
            return False

        if 'reimport' in spec.keys() and spec['reimport'].upper() == 'YES':
            if 'reimport_start' not in spec.keys() or 'reimport_end' not in spec.keys():
                print 'Missing reimport_start or reimport_end attribute(s)'
                return False
            elif spec['reimport_start'] > spec['reimport_end']:
                print 'reimport_start cannot be greater than reimport_end'
                return False

        if 'file_post_type' in spec.keys():
            if (spec['file_post_type'].upper() == 'SQL' and 'file_post_sql' not in spec.keys())\
                or (spec['file_post_type'].upper() == 'SCRIPT' and 'file_post_script' not in spec.keys()):
                print 'Missing correct file post processing attribute'
                return False

        if 'src_post_type' in spec.keys():
            if (spec['src_post_type'].upper() == 'SQL' and 'src_post_sql' not in spec.keys())\
                or (spec['src_post_type'].upper() == 'SCRIPT' and 'src_post_script' not in spec.keys()):
                print 'Missing correct source post processing attribute'
                return False

        return True

    source_spec = cfg.TemplateNodeFixed('source_spec', optional = False, validator = source_spec_validator)
    source_spec.add(cfg.TemplateAttributeFixed('method', optional = False,
                                                  validator = lambda x: x in ['local', 'ftp', 'sftp', 'http', 's3']))
    source_spec.add(cfg.TemplateAttributeFixed('file_dir'))
    source_spec.add(cfg.TemplateAttributeFixed('filename', optional = False))
    source_spec.add(cfg.TemplateAttributeFixed('fileext', optional = False))
    source_spec.add(cfg.TemplateAttributeFixed('source_tag', optional=False))
    source_spec.add(cfg.TemplateAttributeFixed('file_date_lag', value_type = int, default_value = 0))
    source_spec.add(cfg.TemplateAttributeFixed('csv_header_row', value_type = int, default_value = 0))
    source_spec.add(cfg.TemplateAttributeFixed('csv_encoding', default_value = 'utf-8'))
    source_spec.add(cfg.TemplateAttributeFixed('csv_sep', default_value = ','))
    source_spec.add(cfg.TemplateAttributeFixed('csv_date_field'))
    source_spec.add(cfg.TemplateAttributeFixed('dest_table', optional = False))
    source_spec.add(cfg.TemplateAttributeFixed('start_date', optional = False))
    source_spec.add(cfg.TemplateAttributeFixed('start_interval', value_type = int, default_value = 0))
    source_spec.add(cfg.TemplateAttributeFixed('granularity', default_value = 'D',
                                                  validator = lambda x: x in ['D', 'H', '30', '15']))
    source_spec.add(cfg.TemplateAttributeFixed('reimport', default_value = 'no',
                                                  validator = lambda x: x.upper() in ['YES', 'NO']))
    source_spec.add(cfg.TemplateAttributeFixed('reimport_start'))
    source_spec.add(cfg.TemplateAttributeFixed('reimport_end'))
    source_list = cfg.get('/sources').keys()
    print source_list
    template.add(cfg.TemplateNodeSet('source_spec', source_spec, source_list))

    def source_field_spec_validator(value):
        sizes = {'identity': 5, 'integer': 2, 'float': 2, 'varchar': 3, 'date': 2, 'datetime': 2}
        if len(value) <2 :
            print 'Field spec cannot be empty or have less than 2 elements'
            return False
        elif value[0] == 'S_filedate':
            if len(value) != 3 or value[1] != 'date':
                print 'S_filedate fieldspec misconfigured'
                return False
        elif value[0] == 'S_constant':
            if len(value) != sizes[value[1]] + 1:
                print '%s fieldspec should have %d attributes' % (value[0], sizes[value[1]] + 1)
                return False
        elif len(value) != sizes[value[1]]:
            print '%s fieldspec should have %d attributes' % (value[0], sizes[value[1]])
            return False
        elif value[0] == 'S_ignore' and value[1] != 'identity':
            print 'S_ignore fieldspec misconfigured'
            return False
        if value[0] == 'S_interval' and value[1] != 'integer':
            print 'S_interval should be an integer data type'
            return False
        elif value[0] == 'S_datetime' and value[1] != 'datetime':
            print 'S_datetime should be a datetime data type'
            return False

        return True

    source_field_spec = cfg.TemplateNodeVariableAttr('field_attr',
                                                        cfg.TemplateAttributeVariable(value_type = list,
                                                                                         validator = source_field_spec_validator),
                                                        optional = False)
    template.add(cfg.TemplateNodeSet('source_field_spec', source_field_spec, [x + '_fields' for x in source_list]))

    print template.sample()
    print template.sample('TEXT')
    assert template.validate(cfg.root())


@raises(KeyError)
def test_add_invalid_path():
    cfg.set('/invalid_section/attribute', 'value')


def test_add_duplicate():
    cfg.set('/', {'new_section':{'attr1':'val1', 'attr2':'val2'}})
    cfg.set('/', {'new_section': {'attr3': 'val1', 'attr4': 'val2'}})
    result = cfg.get('/new_section/attr1')
    cfg.root().delete('/new_section')
    assert result is None


def test_add_node_ok():
    cfg.set('/', cfg.ConfigNode('add_node_ok'))
    result = cfg.get('/add_node_ok')
    print result
    cfg.root().delete('/add_node_ok')
    assert isinstance(result, dict)


def test_add_attr_ok():
    cfg.set('/', cfg.ConfigAttribute('add_attr_ok', True))
    result = cfg.get('/add_attr_ok')
    print result
    cfg.root().delete('/add_attr_ok')
    assert isinstance(result, bool) and result


def test_path_to_obj_invalid():
    result = cfg.get('/general/abc')
    assert result == None


def test_get_ok():
    test_dict = {'new_section':{'attr1':'val1', 'attr2':'val2'}}
    cfg.set('/', test_dict)
    result = cfg.get('/new_section')
    cfg.root().delete('/new_section')
    assert cmp(result, test_dict)


@raises(ValueError)
def test_delete_root():
    cfg.root().delete('/')


@raises(KeyError)
def test_delete_invalid_path():
    cfg.root().delete('/no_such_node/no_such_attribute')


def test_delete_node():
    test_dict = {'new_section': {'attr1': 'val1', 'attr2': 'val2'}}
    cfg.set('/', test_dict)
    cfg.root().delete('/new_section')
    assert cfg.get('/new_section') is None


def test_delete_attr():
    test_dict = {'new_section': {'attr1': 'val1', 'attr2': 'val2'}}
    cfg.set('/', test_dict)
    cfg.root().delete('/new_section/attr1')
    result = cfg.get('/new_section/attr1')
    cfg.root().delete('/new_section')
    assert result is None


def test_search_by_attr_name_no_attr():
    test_dict = {'i':2, 'n':{'b':True, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', 'x', lambda x: x==5)
    cfg.root().delete('/t1')
    assert result == []


def test_search_by_attr_name_no_match():
    test_dict = {'i':2, 'n':{'b':True, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', 'i', lambda x: x == 5)
    cfg.root().delete('/t1')
    assert result == []


def test_search_by_attr_name_match():
    test_dict = {'i':2, 'n':{'b':True, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', 'i', lambda x: x == 2)
    print result
    cfg.root().delete('/t1')
    assert result == ['/t1']


def test_search_by_attr_no_name_match():
    test_dict = {'i':2, 'n':{'b':True, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', None, lambda x: x == 2)
    cfg.root().delete('/t1')
    assert result == ['/t1']


def test_search_by_attr_depth_match():
    test_dict = {'i':2, 'n':{'b':5, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', None, lambda x: x==5, 2)
    print result
    cfg.root().delete('/t1')
    assert result == ['/t1']


def test_search_by_attr_recursive_match():
    test_dict = {'i':2, 'n':{'b':5, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', None, lambda x: x==5, 1, True)
    cfg.root().delete('/t1')
    assert result == ['/t1/n']


def test_search_by_attr_recursive_depth_match():
    test_dict = {'i':2, 'n':{'b':5, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().search('/', None, lambda x: x==5, 2, True)
    print result
    cfg.root().delete('/t1')
    assert result == ['/t1/n', '/t1']


def test_list_nodes():
    test_dict = {'i':2, 'n':{'b':5, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().list_nodes('/t1')
    print ' '.join(result)
    cfg.root().delete('/t1')
    assert result == ['n']


def test_list_nodes_fullpath():
    test_dict = {'i':2, 'n':{'b':5, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().list_nodes('/t1', True)
    print ' '.join(result)
    cfg.root().delete('/t1')
    assert result == ['/t1/n']


def test_list_attributes():
    test_dict = {'i':2, 'n':{'b':5, 'n2':{}}}
    cfg.set('/t1', test_dict)
    result = cfg.root().list_attributes('/t1')
    print ' '.join(result)
    cfg.root().delete('/t1')
    assert result == ['i']


def test_get_immutable():
    global global_val
    test_dict = {'i':2, 'n':{'b': global_val, 'n2': {}}}
    cfg.set('/t1', test_dict)
    cfg_dict = cfg.get('/t1')
    # Should be 5 for both
    print 'Direct get: ' + str(cfg.get('/t1/n/b'))
    print 'Local map: ' + str(cfg_dict['n']['b'])
    # Change value of global variable
    global_val = 10
    result1 = cfg.get('/t1/n/b')
    # Should print 5
    print '1: ' + str(result1)
    # Change value of local map attribute
    cfg_dict['n']['b'] = global_val
    result2 = cfg.get('/t1/n/b')
    # Should still print 5
    print '2: ' + str(result2)
    # Change local map node
    cfg_dict['n'] = {'b': 'abc'}
    result3 = cfg.get('/t1/n/b')
    # Should still print 5
    print '3: ' + str(result3)
    cfg.root().delete('/t1')
    assert result1 == result2 == result3 == 5

def test_node_type_propagation():
    test_dict = {'n1':
                     {'n11':{'n111':{'attr':1}, 'n112':{'attr':1}, 'n113':{'attr':1}},
                      'n12':{'n121':{'attr':1}, 'n122':{'attr':1}, 'n123':{'attr':1}},
                      'n13':{'n131':{'attr':1}, 'n132':{'attr':1}, 'n133':{'attr':1}}}}
    cfg.set('/t1', test_dict)
    cfg.root().set_node_type('AN')
    r = {}
    r['r1'] = cfg.root().node_type == 'AN' # CN->AN (and remains after C->CN upward propagation)
    r['r2'] = cfg.root()._get_obj('/t1').node_type == 'CN' # C->CN (CN->AN downward propagation)
    r['r3'] = cfg.root()._get_obj('/t1/n1').node_type == 'C' # C-> C (C->CN downward propagation)
    cfg.root()._get_obj('/t1/n1/n11/n111').set_node_type('CN')
    r['r4'] = cfg.root()._get_obj('/t1/n1/n11/n111').node_type == 'CN' # C->CN
    r['r5'] = cfg.root()._get_obj('/t1/n1/n11').node_type == 'AN' # C->AN (C->CN upward propagation)
    r['r6'] = cfg.root()._get_obj('/t1/n1').node_type == 'AN'  # C->AN (C->CN upward propagation)
    r['r7'] = cfg.root()._get_obj('/t1/n1/n11/n112').node_type == 'CN' # C->CN (C->AN downward propagation)
    r['r8'] = cfg.root()._get_obj('/t1/n1/n12').node_type == 'CN'  # C->CN (C->AN downward propagation)
    cfg.root().print_fmt()
    try:
        cfg.root()._get_obj('/t1/n1/n11/n111').set_node_type('C')
    except AttributeError:
        cfg.root()._get_obj('/t1/n1/n11/n111').set_node_type('CN')
        r['r9'] = True
    cfg.root()._get_obj('/t1/n1').set_node_type('CN')
    r['r10'] = cfg.root()._get_obj('/t1/n1').node_type == 'CN'  # AN->CN
    r['r11'] = cfg.root()._get_obj('/t1/n1/n12').node_type == 'C'  # CN->C (AN->CN downward propagation)
    r['r12'] = cfg.root()._get_obj('/t1/n1/n11/n112').node_type == 'C'  # CN->C (AN->CN downward propagation)
    r['r13'] = cfg.root()._get_obj('/t1/n1/n11').node_type == 'C'  # AN->C (AN->CN downward propagation
    cfg.root().delete('/t1')
    print r
    assert len([x for x in r.values() if not x])==0


def test_write_cfg():
    if os.path.isfile('test_cfg.cfg'):
        os.system('rm -f test_cfg.cfg')
    cfg.root().print_fmt()
    cfg.root().write().cfg('test_cfg.cfg')
    new_config = config.ConfigNode.read().cfg('test_cfg.cfg')
    if os.path.isfile('test_cfg.cfg'):
        os.system('rm -f test_cfg.cfg')
    assert cfg.get('/general/log_level') == new_config.get('/general/log_level')


def test_write_json():
    if os.path.isfile('test_cfg.json'):
        os.system('rm -f test_cfg.json')
    cfg.root().write().json('test_cfg.json')
    new_config = config.ConfigNode.read().json('test_cfg.json')
    if os.path.isfile('test_cfg.json'):
        os.system('rm -f test_cfg.json')
    assert cfg.get('/general/log_level') == new_config.get('/general/log_level')
