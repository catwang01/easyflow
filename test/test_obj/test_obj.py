from easyflow.obj import Module, CommandProcessor, NormalFileData, Workflow
from easyflow.common.utils import platform
from test.utils import rmFiles, assertFile
import os
thisDir = os.path.dirname(__file__)
os.chdir(thisDir)

class TestObj:

    def test_case1(self):
        # a -> output
        rmFiles(["output.txt"])
        processor = CommandProcessor("pa", "echo hello world > output.txt")
        a = Module('a', processor, checkInterval=3)
        output = NormalFileData('output', 'output.txt')
        a.addOutput(output)

        workflow = Workflow()
        workflow.addNode(a)
        workflow.setStartNode(a)
        workflow.run()
        if platform() == "Darwin":
            assertFile('output.txt', 'hello world\n')
        else:
            assertFile('output.txt', 'hello world \n')

    def test_case2(self):
        # a -> output1 -> b -> output2
        rmFiles(['output1.txt', 'output2.txt'])
        a = Module('a', 
                        CommandProcessor("pa", "echo hello world1> output1.txt "),
                        checkInterval=3)
        output = NormalFileData('output', 'output1.txt')
        a.addOutput(output)

        b = Module('b', 
                        CommandProcessor("pb", "cat output1.txt > output2.txt && echo hello world2>> output2.txt "),
                        checkInterval=3)
        b.addOutput(NormalFileData('output', 'output2.txt'))
        b.addInput(output)
        assert output.downstreamModules == [b]

        workflow = Workflow()
        workflow.addNode(a)
        workflow.addNode(b)
        workflow.setStartNode(a)
        workflow.run()
        if platform() == "Darwin":
            assertFile('output2.txt', 'hello world1\nhello world2\n')
        else:
            assertFile('output2.txt', 'hello world1 \nhello world2 \n')

    def test_case3(self):
        # a -> output1--> c -> output3
        # b -> output2 /
        rmFiles(['output1.txt', 'output2.txt', 'output3.txt'])
        a = Module('a', CommandProcessor("pa", "sleep 2 && echo output1 > output1.txt"), checkInterval=3)
        b = Module('b', CommandProcessor("pb", "sleep 5 && echo output2 > output2.txt"), checkInterval=3)
        c = Module('c', CommandProcessor("pc", "cat output1.txt > output3.txt && cat output2.txt >> output3.txt"), checkInterval=3)
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
        if platform() == 'Darwin':
            assertFile('output3.txt', 'output1\noutput2\n')
        else:
            assertFile('output3.txt', 'output1 \noutput2 \n')