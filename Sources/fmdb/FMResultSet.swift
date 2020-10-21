public class FMResultSet {
    // MARK: - FMResultSet Private Extension

   /* @interface FMResultSet ()

    - (int)internalStepWithError:(NSError * _Nullable __autoreleasing *)outErr;
    + (instancetype)resultSetWithStatement:(FMStatement *)statement usingParentDatabase:(FMDatabase*)aDB shouldAutoClose:(BOOL)shouldAutoClose;

    @end*/
}

extension FMResultSet: Hashable {

    public func hash(into hasher: inout Hasher) {
        ObjectIdentifier(self).hash(into: &hasher)
    }
}

public func == (lhs: FMResultSet, rhs: FMResultSet) -> Bool {
    return ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
}
