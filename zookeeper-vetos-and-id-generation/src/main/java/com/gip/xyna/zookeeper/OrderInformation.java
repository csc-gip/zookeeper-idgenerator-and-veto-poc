package com.gip.xyna.zookeeper;


import java.io.Serializable;


public class OrderInformation implements Serializable{

  private static final long serialVersionUID = 1L;

  private final Long orderId;
  private final Long rootOrderId;
  private final String destinationKey;


  
  public OrderInformation(Long orderId, Long rootOrderId, String orderType) {
    this.orderId = orderId;
    this.rootOrderId = rootOrderId;
    this.destinationKey = orderType;
  }

  @Override
  public String toString() {
    if( orderId != null && orderId.equals(rootOrderId) ) {
      return "OrderInformation("+orderId+","+destinationKey+")";
    } else {
      return "OrderInformation("+orderId+","+rootOrderId+","+destinationKey+")";
    }
  }

  public Long getOrderId() {
    return orderId;
  }

  public Long getRootOrderId() {
    return rootOrderId;
  }
  
  public String getOrderType() {
    return destinationKey;
  }

  public String getRuntimeContext() {
    return destinationKey;
  }

  public String getDestinationKey() {
    return destinationKey;
  }

}
