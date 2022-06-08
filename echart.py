import json

'''
A simple list of functions to speed up the creation
process of charts with Apache Echart.

TODO: tooltip, legend, grid

@tommasoromano
'''

def date_to_str(dt:list)->list:
    return [str(dt[l]) for l in range(0, len(dt))]

def title(text:str)->dict:
    title = dict()
    title["text"] = text
    return title

def axis_cat(lst:list)->dict:
    '''
    Create Axis category type
    '''
    d = dict()
    d['type'] = 'category'
    d['data'] = lst
    return d

def axis_val(min:int, max:int)->dict:
    '''
    Create Axis value type
    '''
    d = dict()
    d['type'] = 'value'
    d['min'] = min
    d['max'] = max
    return d

def toolbox()->dict:
    toolbox = dict()
    toolbox["feature"] = {"dataZoom":{"yAxisIndex":"none"},"restore":{},"saveAsImage":{}}
    return toolbox

def dataZoom()->list:
    data_zoom = [{"startValue":"2014-06-01"},{"type":"inside"}]
    return data_zoom

def visualMap_piece(gt, lte, color)->dict:
    '''
    - gt
    - lte
    - color
    '''
    vmp = dict()
    vmp['gt'] = gt
    vmp['lte'] = lte
    vmp['color'] = color
    return vmp

def visualMap(pieces:list=None, out_of_range:dict=None, min=None, max=None, text=None, inRange=None)->dict:
    '''
    - pieces
    - out_of_range: es { color: #000000 }
    - min
    - max
    - text: es ['HIGH','LOW']
    - inRange: es { color: ['#f2c31a', '#24b7f2'] }

    TODO
    '''
    #vm = { 'dimension':1, 'orient':'vertical', 
    #    'right':10, 'top':'center', 'calculable':True }
    vm = { 'bottom':10, 'right':'center' }
    #vm['min'] = min
    #vm['max'] = max
    #vm['text'] = text
    #vm['inRange'] = inRange
    if pieces != None:
        vm['pieces'] = pieces
    if out_of_range != None:
        vm['outOfRange'] = out_of_range
    return vm

def serie_label(show:bool=False)->dict:
    '''
    - show: False

    see > https://echarts.apache.org/en/option.html#series-line.label
    '''
    label = dict()
    label["show"] = show
    return label

def lineStyle(color='')->dict:
    '''
    - color

    visit > https://echarts.apache.org/en/option.html#series-scatter.markLine.lineStyle
    '''
    ls = dict()
    if len(color) > 0:
        ls['color'] = color
    return ls

def serie_emphasis(disabled:bool=False, focus:str='none')->dict:
    '''
    Highlight style of the graphic.
    - disabled: bool
    - focus: none, self, focus

    see > https://echarts.apache.org/en/option.html#series-bar.emphasis
    '''
    emphasis = dict()
    emphasis["disabled"] = disabled
    emphasis["focus"] = focus
    return emphasis

def serie_markLine_data(name:str, type='average', xAxis=100, yAxis=100, line_style:dict=None)->dict:
    '''
    - name
    - type: average, min, max, or xAxis, yAxis
    - xAxis
    - yAxis
    '''
    data = dict()
    data['name'] = name
    if type == 'average':
        data['type'] = type
    elif type == 'xAxis':
        data['xAxis'] = xAxis
    elif type == 'yAxis':
        data['yAxis'] = yAxis
    if line_style != None:
        data['lineStyle'] = line_style
    return data

def serie_markLine(data:list, line_style:dict=None)->dict:
    '''
    - data: like { type: 'average', name: 'Avg' }
        or { name: 'Hor line 100', yAxis: 100 }

    visit > https://echarts.apache.org/en/option.html#series-scatter.markLine
    '''
    markLine = dict()
    markLine['data'] = data
    if line_style != None:
        markLine['lineStyle'] = line_style
    return markLine

def serie(name:str, type:str='bar', data:list=None, stack:str='', label:dict=None, markLine:dict=None, emphasis:dict=None)->dict:
    '''
    - name:str
    - type:str - es. bar, line
    - data:list
    - stack:str = the series with the same stack name 
        would be put on top of each other.
    - label:dict
    - markLine:dict
    - emphasis:dict
    '''

    # create dict
    srs = dict()
    srs["name"] = name
    srs["type"] = type
    srs["data"] = data

    if len(stack) > 1:
        srs["stack"] = stack
    if label != None:
        srs["label"] = label
    if markLine != None:
        srs["markLine"] = markLine
    if emphasis != None:
        srs["emphasis"] = emphasis
    return srs

def create(title:dict=None, axis_x:dict=None, axis_y:dict=None, series:list=None,
        toolbox:dict=None, data_zoom:list=None, visual_map:dict=None)->dict:
    '''
    Create the dict useful for JSON of Echart
    - title
    - axis_x
    - axis_y
    - series
    - toolbox
    - data_zoom
    - visual_map
    '''

    # controllo
    if axis_x == None:
        axis_x = axis_val()
    if axis_y == None:
        axis_y = axis_val()

    #
    chrt = dict()
    if title != None:
        chrt["title"] = title
    chrt["tooltip"] = { "trigger":"axis", "axisPointer":{ "type":"shadow" } }
    chrt["grid"] = { "left":"3%", "right":"4%", "bottom":"3%", "containLabel":"true" }
    if toolbox != None:
        chrt["toolbox"] = toolbox
    if data_zoom != None:
        chrt["dataZoom"] = data_zoom
    if visual_map != None:
        chrt['visualMap'] = visual_map
    chrt["xAxis"] = axis_x
    chrt["yAxis"] = axis_y
    chrt["series"] = series

    return chrt

def print_json(chrt:dict, file_path:str)->str:
    '''
    Create a file_path.JSON of chrt dict

    Create a file_path.TXT for copy/paste in
    Echart playground

    return a str of file_path.TXT
    '''
    with open(file_path+".json",'w') as f:
        json.dump(chrt, f, indent=4)

    s = ""

    with open(file_path+".json", 'r') as f:
        s = f.read()

    lines = s.split("\n")
    flines = []
    for line in lines:
        if len(line) <= 1:
            flines.append(line)
            continue
        splt = line.split(":")
        if len(splt) <= 1:
            flines.append(line)
            continue
        lft = splt[0]
        lft = lft.replace("\"","")
        rgt = splt[1]
        rgt = rgt.replace("\"","\'")
        ln = ":".join([lft,rgt])
        flines.append(ln)

    sc = "\n".join(flines)
    sc = "option = " + sc + ";"
    with open(file_path+".txt",'w') as f:
        f.write(sc)

    return sc
