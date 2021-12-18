import os
from typing import List
from easyflow.obj import Module, Data, CommandProcessor, NormalFileData, Workflow
from multiprocessing import Pool
from test.utils import rmFiles, assertFile

class TestObj:

    def test_case1(self):
        # a -> output
        rmFiles(["output.txt"])
        processor = CommandProcessor("echo hello world > output.txt")
        a = Module('a', processor, checkInterval=3)
        output = NormalFileData('output', 'output.txt')
        a.addOutput(output)

        workflow = Workflow()
        workflow.addNode(a)
        workflow.setStartNode(a)
        workflow.run()

        assertFile('output.txt', 'hello world \n')

    def test_case2(self):
        # a -> output1 -> b -> output2
        rmFiles(['output1.txt', 'output2.txt'])
        a = Module('a', 
                        CommandProcessor("echo hello world1> output1.txt "),
                        checkInterval=3)
        output = NormalFileData('output', 'output1.txt')
        a.addOutput(output)

        b = Module('b', 
                        CommandProcessor("cat output1.txt > output2.txt && echo hello world2>> output2.txt "),
                        checkInterval=3)
        b.addOutput(NormalFileData('output', 'output2.txt'))
        b.addInput(output)
        assert output.downstreamModules == [b]

        workflow = Workflow()
        workflow.addNode(a)
        workflow.addNode(b)
        workflow.setStartNode(a)
        workflow.run()
        assertFile('output2.txt', 'hello world1 \nhello world2 \n')

    def test_case3(self):
        # a -> output1--> c -> output3
        # b -> output2 /
        rmFiles(['output1.txt', 'output2.txt', 'output3.txt'])
        a = Module('a', CommandProcessor("sleep 2 && echo output1 > output1.txt"), checkInterval=3)
        b = Module('b', CommandProcessor("sleep 5 && echo output2 > output2.txt"), checkInterval=3)
        c = Module('c', CommandProcessor("cat output1.txt > output3.txt && cat output2.txt >> output3.txt"), checkInterval=3)
        output1 = NormalFileData('output1', 'output1.txt')
        output2 = NormalFileData('output2', 'output2.txt')
        output3 = NormalFileData('output3', 'output3.txt')
        a.addOutput(output1)
        b.addOutput(output2)
        c.addInput(output1)
        c.addInput(output2)
        c.addOutput(output3)

        workflow = Workflow()
        workflow.addNode(a)
        workflow.addNode(b)
        workflow.addNode(c)
        workflow.setStartNode(a)
        workflow.setStartNode(b)
        workflow.run()

        assertFile('output3.txt', 'output1 \noutput2 \n')