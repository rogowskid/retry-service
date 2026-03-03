package com.rogowski.retryservice.mapers;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import org.mapstruct.Mapper;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;

@Mapper(componentModel = "spring")
public interface MessageDataMapper {

    /**
     * Convert getResponse to Spring AMQP Message
     */
    default Message toSpringMessage(GetResponse response) {
        if (response == null)
            return null;

        MessageProperties properties = convertFromBasicProperties(response.getProps());
        return new Message(response.getBody(), properties);
    }

    /**
     * Convert AMQP.BasicProperties to Spring MessageProperties
     */
    default MessageProperties convertFromBasicProperties(AMQP.BasicProperties props) {
        MessageProperties messageProps = new MessageProperties();

        if (props == null)
            return messageProps;

        if (props.getHeaders() != null)
            messageProps.getHeaders().putAll(props.getHeaders());

        messageProps.setMessageId(props.getMessageId());
        messageProps.setContentType(props.getContentType());
        messageProps.setContentEncoding(props.getContentEncoding());
        messageProps.setCorrelationId(props.getCorrelationId());
        messageProps.setReplyTo(props.getReplyTo());
        messageProps.setExpiration(props.getExpiration());
        messageProps.setType(props.getType());
        messageProps.setUserId(props.getUserId());
        messageProps.setAppId(props.getAppId());

        if (props.getDeliveryMode() != null)
            messageProps.setDeliveryMode(
                    props.getDeliveryMode() == 2
                            ? MessageDeliveryMode.PERSISTENT
                            : MessageDeliveryMode.NON_PERSISTENT
            );

        if (props.getPriority() != null)
            messageProps.setPriority(props.getPriority());


        if (props.getTimestamp() != null)
            messageProps.setTimestamp(props.getTimestamp());

        return messageProps;
    }

    /**
     * Convert value from rabbitMQ to String
     *
     * @param value - value
     * @return - convert value
     */
    default String covertValueToString(Object value) {
        return switch (value) {
            case null -> null;
            case LongString ls -> ls.toString();
            case String s -> s;
            default -> String.valueOf(value);
        };
    }
}
