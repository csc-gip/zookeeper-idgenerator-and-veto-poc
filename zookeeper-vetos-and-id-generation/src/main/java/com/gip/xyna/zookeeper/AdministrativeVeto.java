package com.gip.xyna.zookeeper;

public class AdministrativeVeto {

  public static final String ADMIN_VETO_ORDERTYPE = "Administrative Veto";
  public static final Long ADMIN_VETO_ORDERID = -1L;
  public static final OrderInformation ADMIN_VETO_ORDER_INFORMATION = new OrderInformation(ADMIN_VETO_ORDERID,ADMIN_VETO_ORDERID,ADMIN_VETO_ORDERTYPE);
  
  private String name;
  private String documentation;

  public AdministrativeVeto(String name, String documentation) {
    this.name = name;
    this.documentation = documentation;
  }
/*/
  public VetoInformationStorable toVetoInformationStorable(int currentOwnBinding) {
    return new VetoInformationStorable(name, ADMIN_VETO_ORDER_INFORMATION, documentation, currentOwnBinding);
  }
*/
  public String getName() {
    return name;
  }

  public String getDocumentation() {
    return documentation;
  }
}
