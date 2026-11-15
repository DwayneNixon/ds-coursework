
import parameter_server_pb2 as pb


def main():
    prerequisite = pb.TaskId(
        worker_id=7,
        sequence=1
    )

    dependent = pb.TaskId(
        worker_id=8,
        sequence=1
    )

    # Construct a Push with a dependency.
    push = pb.PushRequest(
        keys=[100],
        gradients=[1.0, 2.0],
        width=2,
        task_id=dependent
    )

    push.dependencies.add().CopyFrom(prerequisite)

    # Check serialization and deserialization.
    restored_push = pb.PushRequest()
    restored_push.ParseFromString(
        push.SerializeToString()
    )

    assert len(restored_push.dependencies) == 1
    assert restored_push.dependencies[0] == prerequisite
    assert restored_push.task_id == dependent

    print("Push dependencies preserved: True")

    # Construct a Pull with a dependency.
    pull = pb.PullRequest(
        keys=[100],
        task_id=dependent
    )

    pull.dependencies.add().CopyFrom(prerequisite)

    restored_pull = pb.PullRequest()
    restored_pull.ParseFromString(
        pull.SerializeToString()
    )

    assert len(restored_pull.dependencies) == 1
    assert restored_pull.dependencies[0] == prerequisite
    assert restored_pull.task_id == dependent

    print("Pull dependencies preserved: True")

    # Existing requests without dependency fields
    # must still be valid Protobuf messages.
    old_style = pb.PullRequest(keys=[100])

    assert len(old_style.dependencies) == 0
    assert not old_style.HasField("task_id")

    print("Old request format preserved: True")

    print("Stage 9B.2 protocol checks passed!")


if __name__ == "__main__":
    main()
