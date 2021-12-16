from __future__ import annotations
import logging

import os
from time import sleep
from typing import List, Optional, Union, ClassVar, Dict, Type, Optional, Iterable
from queue import Queue, Empty

from easyflow.common.logger import setupLogger
from easyflow.common.utils import Timer
import threading

logger = setupLogger(__name__)

class ProcessorFactory:

    processorDict: Dict[str, Type[Processor]] = {}

    @classmethod
    def getProcessor(cls: Type['ProcessorFactory'], processorType: str) -> Type[Processor]:
        if processorType in cls.processorDict:
            return cls.processorDict[processorType]
        raise Exception()

    @classmethod
    def register(cls, class_: Type[Processor]) -> Type[Processor]:
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


class Module:

    def __init__(self, name: str,
                 processor: Processor,
                 inputs: Optional[List[Data]] = None,
                 outputs: Optional[List[Data]] = None,
                 checkInterval: int = 10) -> None:
        self.name = name
        self.inputs: List[Data] = []
        if inputs:
            for inputNode in inputs:
                self.addInput(inputNode)
        self.outputs: List[Data] = []
        if outputs:
            for outputNode in outputs:
                self.addOutput(outputNode)
        self.processor = processor
        self.checkInterval = checkInterval 
        # To avoid this module ran by multiple inputNode.
        self.running = False

    def addInput(self, inputNode: Data) -> None:
        self.inputs.append(inputNode)
        inputNode.addDownStream(self)

    def addOutput(self, outputNode: Data) -> None:
        self.outputs.append(outputNode)

    def setWorkflow(self, workflow) -> None:
        self.workflow = workflow

    def _run(self, reportError: bool = False, *args, **kwargs) -> int:
        notExists: List[Data] = []
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


class DataFactory:

    dataTypes: ClassVar[Dict[str, Type[Data]]] = {}

    @classmethod
    def getData(cls, dataNodeType: str) -> Type[Data]:
        if dataNodeType in cls.dataTypes:
            return cls.dataTypes[dataNodeType]
        raise Exception(f"No such dataNodeType: {dataNodeType}")
    
    @classmethod
    def register(cls, dataClass: Type[Data]) -> Type[Data]:
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

class Workflow:

    def __init__(self, 
                    modules: Optional[Dict[str, Module]]=None,
                    datas: Optional[Dict[str, Data]]=None,
                    processors: Optional[Dict[str, Processor]]=None,
                    startNodes: Optional[List[Module]]=None) -> None:
        super().__init__()
        self.modules: Dict[str, Module] = {}
        self.nFinished = 0 
        if modules:
            for node in modules.values():
                self.addNode(node)
        self.datas: Dict[str, Data] = {} if not datas else datas
        self.startNodes: List[Module] = [] if not startNodes else startNodes
        self.processors: Dict[str, Processor] = {} if not processors else processors
        self.queue = Queue() # type:ignore

    def setStartNode(self, moduleNode: Module) -> None:
        self.startNodes.append(moduleNode)

    def addNode(self, node: Union[Module, Data]) -> None:
        if isinstance(node, Data):
            self.datas[node.name] = node
        if isinstance(node, Module):
            self.modules[node.name] = node
            node.setWorkflow(self)
    
    def addNodes(self, nodes: Iterable[Union[Module, Data]]) -> None:
        for node in nodes:
            self.addNode(node)

    def addNodeToQueue(self, node: Module):
        self.queue.put((lambda node: node.run(), (node,), {}))

    def run(self, *args, **kwargs) -> None:
        logger.info("Workflow start!")

        class Logger:
            def write(self, messages: str):
                for mess in messages.strip('\n').split('\n'):
                    logger.info(mess)

        with Timer(stdout=Logger()):
            workers = []
            for i in range(10):
                worker = Worker(i, self)
                workers.append(worker)
                worker.start()
            logger.debug("All workers started!")

            for node in self.startNodes:
                self.addNodeToQueue(node)

            for worker in workers:
                worker.join()
            logger.info("Workflow finished!")

class Worker(threading.Thread):

    def __init__(self, i: int, workflow: Workflow):
        super().__init__()
        self.i = i
        self.workflow = workflow
        self.nFinished = 0

    def log(self, message, severity=logging.INFO):
        if severity == logging.INFO:
            logger.info(f"[Worker{self.i}]{message}")
        else:
            logger.debug(f"[Worker{self.i}]{message}")
    
    def debug(self, message):
        self.log(message, severity=logging.DEBUG)

    def run(self):
        self.debug(f"Starts to work")
        while self.workflow.nFinished != len(self.workflow.modules):
            if self.workflow.nFinished == len(self.workflow.modules):
                self.log(f"[{self.nFinished}/{self.workflow.nFinished}] jobs are finished!")
                break
            try:
                with Timer(descStart="Job start to run!", descEnd="Job end to run!") as timeUsed:
                    func, args, kwargs = self.workflow.queue.get(timeout=5)
                    self.debug(f"func:{func}\nargs: {args}\nkwargs: {kwargs}")
                self.debug(f"Time used: {timeUsed}")
            except Empty:
                self.debug("Wait to get job")
                continue
            except Exception as e:
                raise Exception(f'[Worker{self.i}]Bad execution: %s' % str(e))
            try:
                func(*args,**kwargs)
            except Exception as e:
                raise Exception(f'[Worker{self.i}]Bad execution: %s' % str(e))
            else:
                self.workflow.nFinished += 1
                self.nFinished += 1
