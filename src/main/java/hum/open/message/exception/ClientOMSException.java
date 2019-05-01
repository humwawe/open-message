package hum.open.message.exception;

/**
 * @author hum
 */
public class ClientOMSException extends OMSRuntimeException {

    public String message;

    public ClientOMSException(String message) {
        this.message = message;
    }


    @Override
    public String getMessage() {
        return message;
    }


}
