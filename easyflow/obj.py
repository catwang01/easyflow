import os
from time import sleep
from typing import TypeVar, List, Optional, Generic, Callable, Union, ClassVar, Dict, Type, Optional, Iterable
from queue import Queue

from threading import Thread
from easyflow.common.logger import setupLogger
from easyflow.common.utils import multiprocessingRun
import threading

TData = TypeVar('TData', bound="Data")
TProcessor = TypeVar('TProcessor', bound="Processor")

logger = setupLogger(__name__)

class ProcessorFactory(Generic[TProcessor]):

    processorDict: Dict[str, TProcessor] = {}

    @classmethod
    def getProcessor(cls: Type['ProcessorFactory'], processorType: str) -> TProcessor:
        if processorType in cls.processorDict:
            return cls.processorDict[processorType]
        raise Exception()

    @classmethod
    def register(cls, class_: Type[TProcessor]) -> Type[TProcessor]:
        cls.processorDict[class_.type] = class_
        return class_

class Processor:

    type: ClassVar[str] = ""
    def __init__(self, name) -> None:
        self.name = name

    def run(self) -> None:
        pass

@ProcessorFactory.register
class EmptyProcessor(Processor):
    type: ClassVar[str] = "EmptyProcessor"

    def run(self) -> None:
        return 

@ProcessorFactory.register
class CommandProcessor(Processor):

    type: ClassVar[str]  = "CommandProcessor"

    def __init__(self, name, command: Union[list, str]):
        super().__init__(name)
        self.command: str
        if isinstance(command, list):
            self.command = " && ".join(command)
        else:
            self.command = command

    def run(self) -> None:
        os.system(self.command)


class Module(Generic[TData]):

    def __init__(self, name: str,
                 processor: TProcessor,
                 inputs: Optional[List[TData]] = None,
                 outputs: Optional[List[TData]] = None,
                 checkInterval: int = 10) -> None:
        self.name = name
        self.inputs: List[TData] = []
        if inputs:
            for inputNode in inputs:
                self.addInput(inputNode)
        self.outputs: List[TData] = []
        if outputs:
            for outputNode in outputs:
                self.addOutput(outputNode)
        self.processor = processor
        self.checkInterval = checkInterval 
        # To avoid this module ran by multiple inputNode.
        self.running = False

    def addInput(self, inputNode: TData) -> None:
        self.inputs.append(inputNode)
        inputNode.addDownStream(self)

    def addOutput(self, outputNode: TData) -> None:
        self.outputs.append(outputNode)

    def setWorkflow(self, workflow) -> None:
        self.workflow = workflow

    def _run(self, reportError: bool = False, *args, **kwargs) -> int:
        notExists: List[TData] = []
        for inputNode in self.inputs:
            if not inputNode.checkExists():
                notExists.append(inputNode)
        if notExists:
            if reportError:
                raise Exception(f"The following inputs are detected as nonexisting node: {notExists}")
            else:
                print(f"Module {self.name} failed to run, errorCode: -1")
                return -1
        self.processor.run()
        return 0

    def run(self, *args, **kwargs) -> int:
        verbose = kwargs.get('verbose', True)
        errorCode = -1
        while True:
            errorCode = self._run(*args, **kwargs)
            if errorCode != 0:
                sleep(self.checkInterval)
            else:
                if verbose:
                    print(f"Module: {self.name} ran successfully!")
                for node in self.outputs:
                    for module in node.downstreamModules:
                        if not module.running:
                            self.workflow.addNodeToQueue(module)
                            module.running = True
                break
        return errorCode


class DataFactory(Generic[TData]):

    dataTypes: ClassVar[Dict[str, TData]] = {}

    @classmethod
    def getData(cls, dataNodeType: str) -> TData:
        if dataNodeType in cls.dataTypes:
            return cls.dataTypes[dataNodeType]
        raise Exception(f"No such dataNodeType: {dataNodeType}")
    
    @classmethod
    def register(cls, dataClass: Type[TData]) -> Type[TData]:
        cls.dataTypes[dataClass.type] = dataClass
        return dataClass

class Data:

    type: ClassVar[str] = ""

    def __init__(self, name: str):
        self.name = name
        self.time: int = -1
        self.downstreamModules: List[Module] = []

    def addDownStream(self, downStream: Module):
        self.downstreamModules.append(downStream)

    def checkExists(self) -> bool:
        pass

@DataFactory.register
class NormalFileData(Data):

    type: ClassVar[str] = "NormalFileData"

    def __init__(self, name: str, path: str) -> None:
        super().__init__(name)
        self.path = path

    def checkExists(self) -> bool:
        return os.path.exists(self.path)

def func(node, pool):
    node.run(pool=pool)

class Workflow(Generic[TData, TProcessor]):

    def __init__(self, 
                    modules: Optional[Dict[str, Module]]=None,
                    datas: Optional[Dict[str, TData]]=None,
                    processors: Optional[Dict[str, TProcessor]]=None,
                    startNodes: Optional[List[Module]]=None) -> None:
        super().__init__()
        self.modules: Dict[str, Module] = {}
        self.nFinished = 0 
        if modules:
            for node in modules.values():
                self.addNode(node)
        self.datas: Dict[str, TData] = {} if not datas else datas
        self.startNodes: List[Module] = [] if not startNodes else startNodes
        self.processors: Dict[str, TProcessor] = {} if not processors else processors
        self.queue = Queue()

    def setStartNode(self, moduleNode: Module) -> None:
        self.startNodes.append(moduleNode)
    
    def addNode(self, node: Union[Module, TData]) -> None:
        if isinstance(node, Data):
            self.datas[node.name] = node
        if isinstance(node, Module):
            self.modules[node.name] = node
            node.setWorkflow(self)
    
    def addNodes(self, nodes: Iterable[Union[Module, TData]]) -> None:
        for node in nodes:
            self.addNode(node)

    def addNodeToQueue(self, node: Module):
        self.queue.put((lambda node: node.run(), (node,), {}))

    def run(self, *args, **kwargs) -> None:
        workers = []
        for i in range(10):
            worker = Worker(i, self)
            workers.append(worker)
            worker.start()

        for node in self.startNodes:
            self.addNodeToQueue(node)

        for worker in workers:
            worker.join()
        logger.info("Workflow finished!")
        

class Worker(threading.Thread):

    def __init__(self, i: int, workflow: 'Workflow'):
        super().__init__()
        self.i = i
        self.workflow = workflow

    def run(self):
        logger.debug(f"worker: {self.i} start to work")
        while self.workflow.nFinished != len(self.workflow.modules):
            if self.workflow.nFinished == len(self.workflow.modules):
                logger.info("All job finished!")
                break
            try:
                func, args, kwargs = self.workflow.queue.get(timeout=5)
            except Exception:
                continue
            try:
                func(*args,**kwargs)
            except Exception as e:
                raise Exception('bad execution: %s' % str(e))
            else:
                self.workflow.nFinished += 1