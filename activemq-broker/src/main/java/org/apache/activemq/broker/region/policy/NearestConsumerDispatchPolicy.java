package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class NearestConsumerDispatchPolicy extends SimpleDispatchPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(NearestConsumerDispatchPolicy.class);

    private final Map<String, ConsumerTracker> consumerTrackers = new HashMap<>();
    private final AtomicLong dispatchCounter = new AtomicLong(0);

    @Override
    public boolean dispatch(MessageReference node,
                            MessageEvaluationContext msgContext,
                            List<Subscription> consumers) throws Exception {
        LOG.info("Dispatching message: {}", node.getMessageId());

        if (consumers.isEmpty()) {
            LOG.warn("No consumers available");
            return false;
        }

        // Tạo tracker cho các consumer nếu chưa có
        for (Subscription sub : consumers) {
            String consumerId = sub.getConsumerInfo().getConsumerId().toString();
            consumerTrackers.putIfAbsent(consumerId, new ConsumerTracker());
        }

        // Chọn consumer tối ưu
        Subscription selectedConsumer = selectOptimalConsumer(consumers, node);
        if (selectedConsumer != null) {
            String consumerId = selectedConsumer.getConsumerInfo().getConsumerId().toString();
            ConsumerTracker tracker = consumerTrackers.get(consumerId);
            if (tracker != null) {
                tracker.messageDispatched(); // Cập nhật trạng thái của consumer
            }

            LOG.info("Selected consumer: {}", consumerId);
            return super.dispatch(node, msgContext, Collections.singletonList(selectedConsumer));
        } else {
            LOG.warn("Fallback to round-robin dispatch");
            long count = dispatchCounter.getAndIncrement();
            int index = (int)(count % consumers.size());
            return super.dispatch(node, msgContext, Collections.singletonList(consumers.get(index)));
        }
    }

    private Subscription selectOptimalConsumer(List<Subscription> consumers, MessageReference node) {
        Message message = node.getMessage();
        Map<Subscription, Double> scoreMap = new HashMap<>();

        for (Subscription sub : consumers) {
            ConsumerInfo info = sub.getConsumerInfo();
            String consumerId = info.getConsumerId().toString();
            ConsumerTracker tracker = consumerTrackers.get(consumerId);

            if (tracker == null) {
                continue;
            }

            double score = 0.0;

            // Điểm số ưu tiên theo consumer
            switch (info.getPriority()) {
                case ConsumerInfo.HIGH_PRIORITY:
                    score -= 5.0;
                    break;
                case ConsumerInfo.NORMAL_PRIORITY:
                    score -= 2.0;
                    break;
                case ConsumerInfo.LOW_PRIORITY:
                    score += 5.0;
                    break;
            }

            // Điểm số theo tải
            int pendingMessages = sub.getInFlightSize();
            double loadFactor = (double) pendingMessages / Math.max(1, info.getPrefetchSize());
            score += (loadFactor * 15.0);

            // Điểm số theo thời gian chờ
            long timeSinceLastDispatch = tracker.getTimeSinceLastDispatch();
            score -= timeSinceLastDispatch / 1000.0; // Trừ điểm nếu consumer lâu chưa nhận tin nhắn

            scoreMap.put(sub, score);
        }

        return scoreMap.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    private static class ConsumerTracker {
        private long lastDispatchTime = System.currentTimeMillis();

        public void messageDispatched() {
            lastDispatchTime = System.currentTimeMillis();
        }

        public long getTimeSinceLastDispatch() {
            return System.currentTimeMillis() - lastDispatchTime;
        }
    }
}
