from easyflow.obj import Workflow, DataFactory, Module, Processor, ProcessorFactory, Data
from typing import Dict, List

class Resolver:

    def resolve(self, wdef, *args, **kwargs) -> Workflow:
        pass
    
class JsonResolver(Resolver):

    def resolve(self, wdef, *args, **kwargs) -> Workflow:
        json = wdef
        datas: Dict[str, Data] = {}
        for name, dataArgs in json['datas'].items():
            if 'type' in dataArgs:
                dataType = dataArgs['type']
                del dataArgs['type']
            else:
                dataType = 'NormalFileData'
            dataNodeClass = DataFactory.getData(dataType)
            datas[name] = dataNodeClass(name=name, **dataArgs)

        processors: Dict[str, Processor] = {}
        for name, processorArgs in json['processors'].items():
            if 'type' in processorArgs:
                processorType = processorArgs['type']
                del processorArgs['type']
            else:
                processorType = 'NormalFileData'
            processorClass = ProcessorFactory.getProcessor(processorType)
            processors[name] = processorClass(name=name, **processorArgs)

        modules: Dict[str, Module] = {}
        for name, moduleArgs in json['modules'].items():
            inputs: List[Data] = [datas[dataName] for dataName in moduleArgs.get("inputs", [])]
            outputs: List[Data] = [datas[dataName] for dataName in moduleArgs.get("outputs", [])]
            processor = processors[moduleArgs['processor']]
            modules[name] = Module(name, processor, inputs, outputs)

        startNodes = [modules[moduleName] for moduleName in json.get("startNodes", [])]
        return Workflow(datas=datas, modules=modules, processors=processors, startNodes=startNodes)
        
        
