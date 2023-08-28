package com.gip.xyna.zookeeper;


public class VetoAllocationResult {
  public static final VetoAllocationResult SUCCESS = new VetoAllocationResult(true,null);
  public static final VetoAllocationResult FAILED = new VetoAllocationResult(false,"WAITING_FOR_VETO");
  public static final VetoAllocationResult UNSUPPORTED = new 
      VetoAllocationResult( new VetoInformation(new AdministrativeVeto("vetos currently unsupported","vetos currently unsupported"),0));
  
  private boolean allocated;

  private String xynaException;
  private String status;
  private VetoInformation existingVeto;

  public VetoAllocationResult(boolean allocated,String status) {
    this.allocated = allocated;
    this.status = status;
  }
  
  public VetoAllocationResult(String xynaException) {
    this.allocated = false;
    this.xynaException = xynaException;
  }

  public VetoAllocationResult(VetoInformation existing) {
    this.allocated = false;
    this.existingVeto = existing;
    this.status = existing.isAdministrative() ? "WAITING_FOR_VETO" : "SCHEDULING_VETO";
  }

  public boolean isAllocated() {
    return allocated;
  }

  public String getVetoName() {
    if( existingVeto != null ) {
      return existingVeto.getName();
    }
    return null;
  }

  @Override
  public String toString() {
    return new StringBuilder("VetoAllocationResult(allocated=").append(allocated).append(",name=").append(getVetoName())
        .append(",holdBy=").append(existingVeto != null ? existingVeto.getUsingOrderId() : -1).append(")").toString();
  }

  public String getXynaException() {
    return xynaException;
  }

  public String getOrderInstanceStatus() {
    return status;
  }

  public VetoInformation getExistingVeto() {
    return existingVeto;
  }

}
