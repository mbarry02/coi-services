#!/usr/bin/env python

"""
@package  ion.util.agent_launcher
@author   Ian Katz
"""

from interface.objects import ProcessSchedule, ProcessRestartMode, ProcessQueueingMode, ProcessStateEnum
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from pyon.core.exception import BadRequest

from ooi.logging import log


class AgentLauncher(object):

    def __init__(self, process_dispatcher_client):
        self.process_dispatcher_client = process_dispatcher_client


    def launch(self, agent_config, process_definition_id):
        """
        schedule the launch
        """

        log.debug("schedule agent process")
        process_schedule = ProcessSchedule(restart_mode=ProcessRestartMode.ABNORMAL,
                                           queueing_mode=ProcessQueueingMode.ALWAYS)
        process_id = self.process_dispatcher_client.schedule_process(process_definition_id=process_definition_id,
                                                                      schedule=process_schedule,
                                                                      configuration=agent_config)

        return process_id


    def await_launch(self, process_id, timeout):

        log.debug("waiting %s seconds for agent launch", timeout)
        psg = ProcessStateGate(self.process_dispatcher_client.read_process, process_id, ProcessStateEnum.RUNNING)
        if not psg.await(20):
            # todo: different error
            raise BadRequest("The agent process '%s' failed to launch in %s seconds" %
                             (process_id, timeout))
