package org.clever.canal.parse.inbound.group;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.parse.CanalEventParser;

import java.util.ArrayList;
import java.util.List;

/**
 * 组合多个EventParser进行合并处理，group只是做为一个delegate处理
 */
@SuppressWarnings("unused")
public class GroupEventParser extends AbstractCanalLifeCycle implements CanalEventParser {

    private List<CanalEventParser> eventParsers = new ArrayList<>();

    public void start() {
        super.start();
        // 统一启动
        for (CanalEventParser eventParser : eventParsers) {
            if (!eventParser.isStart()) {
                eventParser.start();
            }
        }
    }

    public void stop() {
        super.stop();
        // 统一关闭
        for (CanalEventParser eventParser : eventParsers) {
            if (eventParser.isStart()) {
                eventParser.stop();
            }
        }
    }

    public void setEventParsers(List<CanalEventParser> eventParsers) {
        this.eventParsers = eventParsers;
    }

    public void addEventParser(CanalEventParser eventParser) {
        if (!eventParsers.contains(eventParser)) {
            eventParsers.add(eventParser);
        }
    }

    public void removeEventParser(CanalEventParser eventParser) {
        eventParsers.remove(eventParser);
    }

    public List<CanalEventParser> getEventParsers() {
        return eventParsers;
    }
}
