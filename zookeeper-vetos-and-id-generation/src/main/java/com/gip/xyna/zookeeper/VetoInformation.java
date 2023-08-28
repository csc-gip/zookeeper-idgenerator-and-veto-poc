package com.gip.xyna.zookeeper;


import java.io.Serializable;

public class VetoInformation implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final OrderInformation usingOrder;
  private final boolean administrative;
  private final int binding;
  private String documentation;
  
  public VetoInformation(AdministrativeVeto administrativeVeto, int binding) {
    this.name = administrativeVeto.getName();
    this.usingOrder = null;
    this.administrative = true;
    this.binding =  binding;
    this.documentation = administrativeVeto.getDocumentation();
  }

  public VetoInformation(String name, OrderInformation usingOrder, int binding) {
    this.name = name;
    this.usingOrder = usingOrder;
    this.administrative = false;
    this.binding =  binding;
  }

  public VetoInformation(String name, OrderInformation usingOrder, String documentation, int binding) {
    this.name = name;
    this.usingOrder = usingOrder;
    this.documentation = documentation;
    this.administrative = AdministrativeVeto.ADMIN_VETO_ORDERID.equals(usingOrder.getOrderId());
    this.binding = binding;
  }
  
  public VetoInformation(String name) {
    this.name = name;
    this.usingOrder = null;
    this.administrative = false;
    this.binding = 0;
  }
  
  @Override
  public String toString() {
    if( administrative ) {
      return "VetoInformation("+name+": "+documentation+")";
    } else {
      if( binding != 0 ) {
        return "VetoInformation("+binding+"-"+name+": "+usingOrder+")";
      } else {
        return "VetoInformation("+name+": "+usingOrder+")";
      }
    }
  } 
  
  public OrderInformation getOrderInformation() {
    if( usingOrder != null ) {
      return usingOrder;
    } else {
      return AdministrativeVeto.ADMIN_VETO_ORDER_INFORMATION;
    }
  }
  

  public Long getUsingOrderId() {
    if( usingOrder != null ) {
      return usingOrder.getOrderId();
    } else {
      return AdministrativeVeto.ADMIN_VETO_ORDERID;
    }
  }
  
  public Long getUsingRootOrderId() {
    if( usingOrder != null ) {
      return usingOrder.getRootOrderId();
    } else {
      return AdministrativeVeto.ADMIN_VETO_ORDERID;
    }
  }

  public boolean isAdministrative() {
    return administrative;
  }
  
  public String getName() {
    return name;
  }

  //package private
  void setDocumentation(String documentation) {
    this.documentation = documentation;
  }

  public String getDocumentation() {
    if( administrative ) {
      return documentation;
    } else {
      return usingOrder.getRuntimeContext();
    }
  }
  
  public String getUsingOrderType() {
    if( usingOrder != null ) {
      return usingOrder.getOrderType();
    } else {
      return AdministrativeVeto.ADMIN_VETO_ORDERTYPE;
    }
  }

  public int getBinding() {
    return binding;
  }

  public OrderInformation getUsingOrder() {
    return usingOrder;
  }

}
