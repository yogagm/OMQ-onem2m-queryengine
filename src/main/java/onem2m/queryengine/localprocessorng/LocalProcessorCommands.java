package onem2m.queryengine.localprocessorng;

import onem2m.queryengine.common.Query;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import java.util.HashMap;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface LocalProcessorCommands {
    @WebMethod
    public HashMap<String, String> addSubQuery(Query sq);
}
