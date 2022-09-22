import ijson
import csv
import uuid
import glob
import os

in_network_file = "PUT_THE_IN_NETWORK_FILE_HERE.json"

def pop(string):
    """
    A "string.like.this" becomes just "this".
    ("pop" off the last part.)
    """
    return string.split('.')[-1]

def walk(prefix, parser, output_dir, **uuids):
    """
    Walk the JSON rows and write the chunks to file.

    The ijson parser produces rows that are prefixed
    by their location in the JSON. For example you might
    see a prefix like

    parent.item.child.item.grandchild

    Anything at the same level gets written to the same
    file. To make the files readable I've stripped off
    everything that comes before the last dot.

    Similarly, a prefix of '' (corresponding to the
    highest level in the JSON) doesn't play nicely
    with writing to file, so I've changed it to "root"
    when needed.
    """
    
    if prefix == '':
        prefix = 'root'
    
    data = {}
    
    # Add a new UUID field
    uuids[f'{pop(prefix)}_uuid'] = uuid.uuid4()

    # Loop through all parent and child UUID
    # fields and add to row
    for key, value in uuids.items():
        data[key] = value

    while True:
        
        new_row = next(parser)
        new_prefix, new_event, new_value = new_row
        
        if new_prefix == '':
            new_prefix = 'root'

        if new_event == 'start_map':
            # Recurse to the next level down,
            # passing in the uuids from the parents
            walk(new_prefix, parser, output_dir, **uuids)
            
        if new_event in ['string', 'number']:
            data[pop(new_prefix)] = new_value
            
        if (new_event == 'end_map') and (new_prefix == prefix):
            # Once we've reached "end map" and the prefix
            # matches, we've captured everything at this level
            # in the JSON. Write it to file.
                
            output_filename = f"{output_dir}/{prefix}.csv"
            
            mode = "w+"

            if os.path.exists(output_filename):
                mode = "a"
                
            with open(output_filename, mode) as f:
                fieldnames = [pop(k) for k in data.keys()]
                writer = csv.DictWriter(f, fieldnames = fieldnames)
                
                if mode == "w+":
                    writer.writeheader()
                    
                writer.writerow(data)
                
            break
        

def tableize_file(filename, output_dir = './flatten'):
    
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
        
    else:
        for file in glob.glob(f'output_dir/*'):
            os.rmdir(file)

    with open(filename, "r") as f:
    
        parser = ijson.parse(f)
        prefix, event, value = next(parser)

        walk(prefix, parser, output_dir)

        
tableize_file(in_network_file)