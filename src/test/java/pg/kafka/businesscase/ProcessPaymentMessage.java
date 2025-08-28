package pg.kafka.businesscase;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import pg.kafka.message.Message;

import java.time.LocalDateTime;


@NoArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ProcessPaymentMessage extends Message {
    private String paymentId;
    private String orderId;
    private String paymentStatus;
    private LocalDateTime paymentDate;

    public ProcessPaymentMessage(final String paymentId,
                                 final String orderId,
                                 final String paymentStatus,
                                 final LocalDateTime paymentDate) {
        super(paymentDate);
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.paymentStatus = paymentStatus;
        this.paymentDate = paymentDate;
    }
}
