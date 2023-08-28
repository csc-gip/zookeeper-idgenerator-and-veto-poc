package com.gip.xyna.zookeeper;

import java.util.Arrays;

public class App {

    private static final String DEFAULT_REALM = "default";

    private static int DEFAULT_INCREMENT = 50_000;

    public static void main(String[] args) {

        IDGenerationAlgorithmUsingZookeeper idgeneration = new IDGenerationAlgorithmUsingZookeeper();

        idgeneration.init();

        for (int i = 0; i < 2 * DEFAULT_INCREMENT; ++i) {
            System.out.println("Counter 1 value after increment: " + idgeneration.getUniqueId(DEFAULT_REALM));
            System.out.println("Counter 2 value after increment: "
                    + idgeneration.getUniqueId("Test Realm mit schönen.Sonderzeichen! / und mehr Text"));
        }

        idgeneration.shutdown();

        VM_Zookeeper vm = new VM_Zookeeper();
        vm.init();

        OrderInformation oi1 = new OrderInformation(1L, 1L, "Type 1");
        OrderInformation oi2 = new OrderInformation(2L, 1L, "Type 2");
        VetoAllocationResult r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);
        System.out.println(r);

        r = vm.allocateVetos(oi1, Arrays.asList("Veto 1"), 0);
        System.out.println(r);

        r = vm.allocateVetos(oi2, Arrays.asList("Veto 1", "Veto 2"), 0);
        System.out.println(r);

        r = vm.allocateVetos(oi2, Arrays.asList("Veto 2", "Veto 3"), 0);
        System.out.println(r);


        vm.listVetos().stream().forEach(v -> System.out.println(v));

        vm.undoAllocation(oi1, Arrays.asList("Veto 1"));

        vm.undoAllocation(oi2, Arrays.asList("Veto 2"));
        vm.undoAllocation(oi2, Arrays.asList("Veto 3"));

      /*  vm.freeVetosForced(1);
        vm.freeVetosForced(2);
*/
    }
}