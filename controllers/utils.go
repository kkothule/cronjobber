package controllers

import (
	"fmt"
	"time"

	cronjobberv1alpha1 "github.com/hiddeco/cronjobber/api/v1alpha1"
	"github.com/robfig/cron"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

var (
	//	controllerKind = batchv1.SchemeGroupVersion.WithKind("CronJob")
	controllerKind = cronjobberv1alpha1.GroupVersion
)

func getCurrentTimeInZone(sj *cronjobberv1alpha1.TZCronJob) (time.Time, error) {
	if sj.Spec.TimeZone == "" {
		return time.Now(), nil
	}

	loc, err := time.LoadLocation(sj.Spec.TimeZone)
	if err != nil {
		return time.Now(), err
	}

	return time.Now().In(loc), nil
}

// byJobStartTimeStar sorts a list of jobs by start timestamp, using their names as a tie breaker.
type byJobStartTimeStar []*batchv1.Job

func (o byJobStartTimeStar) Len() int      { return len(o) }
func (o byJobStartTimeStar) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o byJobStartTimeStar) Less(i, j int) bool {
	if o[i].Status.StartTime == nil && o[j].Status.StartTime != nil {
		return false
	}
	if o[i].Status.StartTime != nil && o[j].Status.StartTime == nil {
		return true
	}
	if o[i].Status.StartTime.Equal(o[j].Status.StartTime) {
		return o[i].Name < o[j].Name
	}
	return o[i].Status.StartTime.Before(o[j].Status.StartTime)
}

// Utilities for dealing with Jobs and CronJobs and time.

func inActiveList(cj *cronjobberv1alpha1.TZCronJob, uid types.UID) bool {
	for _, j := range cj.Status.Active {
		if j.UID == uid {
			return true
		}
	}
	return false
}

func getFinishedStatus(j *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

// IsJobFinished returns whether or not a job has completed successfully or failed.
func IsJobFinished(j *batchv1.Job) bool {
	isFinished, _ := getFinishedStatus(j)
	return isFinished
}
func deleteFromActiveList(cj *cronjobberv1alpha1.TZCronJob, uid types.UID) {
	if cj == nil {
		return
	}
	// TODO: @alpatel the memory footprint can may be reduced here by
	//  cj.Status.Active = append(cj.Status.Active[:indexToRemove], cj.Status.Active[indexToRemove:]...)
	newActive := []corev1.ObjectReference{}
	for _, j := range cj.Status.Active {
		if j.UID != uid {
			newActive = append(newActive, j)
		}
	}
	cj.Status.Active = newActive
}

// getNextScheduleTime gets the time of next schedule after last scheduled and before now
//  it returns nil if no unmet schedule times.
// If there are too many (>100) unstarted times, it will raise a warning and but still return
// the list of missed times.
func getNextScheduleTime(cj cronjobberv1alpha1.TZCronJob, now time.Time, schedule cron.Schedule, recorder record.EventRecorder) (*time.Time, error) {
	var (
		earliestTime time.Time
	)
	if cj.Status.LastScheduleTime != nil {
		earliestTime = cj.Status.LastScheduleTime.Time
	} else {
		// If none found, then this is either a recently created cronJob,
		// or the active/completed info was somehow lost (contract for status
		// in kubernetes says it may need to be recreated), or that we have
		// started a job, but have not noticed it yet (distributed systems can
		// have arbitrary delays).  In any case, use the creation time of the
		// CronJob as last known start time.
		earliestTime = cj.ObjectMeta.CreationTimestamp.Time
	}
	if cj.Spec.StartingDeadlineSeconds != nil {
		// Controller is not going to schedule anything below this point
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cj.Spec.StartingDeadlineSeconds))

		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}
	if earliestTime.After(now) {
		return nil, nil
	}

	t, numberOfMissedSchedules, err := getMostRecentScheduleTime(earliestTime, now, schedule)

	if numberOfMissedSchedules > 100 {
		// An object might miss several starts. For example, if
		// controller gets wedged on friday at 5:01pm when everyone has
		// gone home, and someone comes in on tuesday AM and discovers
		// the problem and restarts the controller, then all the hourly
		// jobs, more than 80 of them for one hourly cronJob, should
		// all start running with no further intervention (if the cronJob
		// allows concurrency and late starts).
		//
		// However, if there is a bug somewhere, or incorrect clock
		// on controller's server or apiservers (for setting creationTimestamp)
		// then there could be so many missed start times (it could be off
		// by decades or more), that it would eat up all the CPU and memory
		// of this controller. In that case, we want to not try to list
		// all the missed start times.
		//
		// I've somewhat arbitrarily picked 100, as more than 80,
		// but less than "lots".
		recorder.Eventf(&cj, corev1.EventTypeWarning, "TooManyMissedTimes", "too many missed start times: %d. Set or decrease .spec.startingDeadlineSeconds or check clock skew", numberOfMissedSchedules)
		klog.InfoS("too many missed times", "cronjob", klog.KRef(cj.GetNamespace(), cj.GetName()), "missed times", numberOfMissedSchedules)
	}
	return t, err
}

// getMostRecentScheduleTime returns the latest schedule time between earliestTime and the count of number of
// schedules in between them
func getMostRecentScheduleTime(earliestTime time.Time, now time.Time, schedule cron.Schedule) (*time.Time, int64, error) {
	t1 := schedule.Next(earliestTime)
	t2 := schedule.Next(t1)

	if now.Before(t1) {
		return nil, 0, nil
	}
	if now.Before(t2) {
		return &t1, 1, nil
	}

	// It is possible for cron.ParseStandard("59 23 31 2 *") to return an invalid schedule
	// seconds - 59, minute - 23, hour - 31 (?!)  dom - 2, and dow is optional, clearly 31 is invalid
	// In this case the timeBetweenTwoSchedules will be 0, and we error out the invalid schedule
	timeBetweenTwoSchedules := int64(t2.Sub(t1).Round(time.Second).Seconds())
	if timeBetweenTwoSchedules < 1 {
		return nil, 0, fmt.Errorf("time difference between two schedules less than 1 second")
	}
	timeElapsed := int64(now.Sub(t1).Seconds())
	numberOfMissedSchedules := (timeElapsed / timeBetweenTwoSchedules) + 1
	t := time.Unix(t1.Unix()+((numberOfMissedSchedules-1)*timeBetweenTwoSchedules), 0).UTC()
	return &t, numberOfMissedSchedules, nil
}

// getTimeHash returns Unix Epoch Time in minutes
func getTimeHashInMinutes(scheduledTime time.Time) int64 {
	return scheduledTime.Unix() / 60
}

// getJobFromTemplate2 makes a Job from a CronJob. It converts the unix time into minutes from
// epoch time and concatenates that to the job name, because the cronjob_controller v2 has the lowest
// granularity of 1 minute for scheduling job.
func getJobFromTemplate2(cj *cronjobberv1alpha1.TZCronJob, scheduledTime time.Time) (*batchv1.Job, error) {
	labels := copyLabels(&cj.Spec.JobTemplate)
	annotations := copyAnnotations(&cj.Spec.JobTemplate)
	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	name := getJobName(cj, scheduledTime)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:            labels,
			Annotations:       annotations,
			Name:              name,
			CreationTimestamp: metav1.Time{Time: scheduledTime},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cj, schema.GroupVersionKind{
				Group:   "cronjobber.hidde.co",
				Version: "v1alpha1",
				Kind:    "TZCronJob",
			})},
		},
	}
	cj.Spec.JobTemplate.Spec.DeepCopyInto(&job.Spec)
	return job, nil
}
func copyLabels(template *batchv1.JobTemplateSpec) labels.Set {
	l := make(labels.Set)
	for k, v := range template.Labels {
		l[k] = v
	}
	return l
}

func copyAnnotations(template *batchv1.JobTemplateSpec) labels.Set {
	a := make(labels.Set)
	for k, v := range template.Annotations {
		a[k] = v
	}
	return a
}
