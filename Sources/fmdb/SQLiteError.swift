import Foundation

public enum SQLiteError: Error, LocalizedError {

    case database(message: String)
    case constraint(message: String)

    public var errorDescription: String? {
        switch self {
        case .database (let message), .constraint(message: let message):
            return "\(String(describing: self)): \(message)"
        }
    }

}
