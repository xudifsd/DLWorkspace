import os.path

verbose = False

def expand_path(path):
    return expanduser(path)

# Test if a certain Config entry exist
def fetch_dictionary(dic, entry):
    if isinstance(entry, list):
        # print "Fetch " + str(dic) + "@" + str(entry) + "==" + str( dic[entry[0]] ) 
        if isinstance( dic, list ):
            for subdic in dic:
                if entry[0] in subdic:
                    if len(entry)<=1:
                        return subdic[entry[0]]
                    else:
                        return fetch_dictionary(subdic[entry[0]], entry[1:])
            return None
        elif entry[0] in dic:
            if len(entry)<=1:
                return dic[entry[0]]
            else:
                return fetch_dictionary(dic[entry[0]], entry[1:])
        else:
            return None
    else:
        print "fetch_config expects to take a list, but gets " + str(entry)
    
# Test if a certain Config entry exist
def fetch_config(config, entry):
    return fetch_dictionary( config, entry )
    
# Test if a certain Config entry exist
def fetch_config_and_check(config, entry):
    ret = fetch_config( entry )
    if ret is None:
        print "Error: config entry %s doesn't exist" % entry
        exit()
    return ret

    
# Merge entries in config2 to that of config1, if entries are dictionary. 
# If entry is list or other variable, it will just be replaced. 
# say config1 = { "A" : { "B": 1 } }, config2 = { "A" : { "C": 2 } }
# C python operation: config1.update(config2) give you { "A" : { "C": 2 } }
# merge_config will give you: { "A" : { "B": 1, "C":2 } }
def merge_config( config1, config2 ):
    for entry in config2:
        if entry in config1:
            if isinstance( config1[entry], dict): 
                if isinstance( config2[entry], dict): 
                    merge_config( config1[entry], config2[entry] )
                else:
                    print "Error in configuration: %s should be of type %s, but is written as type %s in configuration" %(entry, type(config1[entry]), type(config2[entry]) )
                    exit(1)
            else:
                config1[entry] = config2[entry]
        else:
            config1[entry] = config2[entry]

# Config, tuple to dictionary
def config_to_dict( inconfig ):
    outconfig = {}
    for k,v in inconfig.iteritems():
        outconfig[ k ] = v
    return outconfig

    
# set a configuration, if an entry in configuration exists and is a certain type, then 
# use that entry, otherwise, use default value
# name: usually a string, the name of the configuration
# entry: a string list, used to mark the entry in the yaml file
# type: expect type of the configuration
# defval: default value
def update_one_config(config, name, entry, type, defval):
    val = fetch_config(config, entry)
    if val is None:
        config[name] = defval
    elif isinstance( val, type ):
        config[name] = val
        if verbose:
            print "config["+name+"]="+str(val)
    else:
        print "Error: Configuration " + name + " needs a " + str(type) +", but is given:" + str(val)

def update_config(config, default_config_mapping):
    apply_config_mapping(config, default_config_mapping)
    
# translate a config entry to another, check type and do format conversion along the way
def translate_config_entry( config, entry, name, type, leading_space = 0 ):
    content = fetch_config( config, entry )
    if not content is None:
        if isinstance( content, type ):
            if leading_space > 0 : 
                adj_content = add_leading_spaces( content, leading_space )
            else:
                adj_content = content
            config[name] = adj_content
            if verbose: 
                print "Configuration entry: " + name
                print adj_content
        else:
            print "In configuration file, " + str( entry ) + " should be type of " +str(type) + ", rather than: "+ str(content )
            exit()

# Certain configuration that is default in system 
def init_config(default_config_parameters):
    config = {}
    for k,v in default_config_parameters.iteritems():
        config[ k ] = v
    return config

def apply_config_mapping(config, default_config_mapping):
    for k,tuple in default_config_mapping.iteritems():
        if not ( k in config ) or len(config[k])<=0:
            dstname = tuple[0]
            value = fetch_config(config, dstname)
            if not (value is None):
                config[k] = tuple[1](value)
                if verbose:
                    print "Config[%s] = %s" %(k, config[k])
