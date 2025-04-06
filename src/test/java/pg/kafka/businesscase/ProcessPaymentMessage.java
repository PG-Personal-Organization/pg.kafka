package pg.kafka.businesscase;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import pg.kafka.message.Message;

import java.time.LocalDateTime;


@RequiredArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ProcessPaymentMessage extends Message {
    private final String paymentId;
    private final String orderId;
    private final String paymentStatus;
    private LocalDateTime paymentDate;

    public ProcessPaymentMessage(String paymentId,
                                 String orderId,
                                 String paymentStatus,
                                 LocalDateTime paymentDate) {
        super(paymentDate);
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.paymentStatus = paymentStatus;
        this.paymentDate = paymentDate;
    }
}
